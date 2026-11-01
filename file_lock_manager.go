// Package filelock provides advisory file locking via flock(2) with timeout,
// context cancellation, and deadlock-free upgrade support.
//
// The package deliberately keeps the surface small: a single Lock type that
// wraps an *os.File and a small set of operations. All blocking operations
// honour a context, so callers can cancel a wait without leaking a goroutine.
package filelock

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"syscall"
	"time"
)

// ErrClosed is returned when an operation is attempted on a Lock that has
// already been released (or whose file has been closed).
var ErrClosed = errors.New("filelock: lock is closed")

// ErrTimeout is returned when a lock cannot be acquired before the timeout
// elapses. It is distinct from context cancellation so callers can tell the
// two apart.
var ErrTimeout = errors.New("filelock: lock acquisition timed out")

// ErrWouldDeadlock is returned by TryUpgrade when the upgrade cannot be
// performed without risking a deadlock. The caller is expected to release
// the shared lock and retry with an exclusive lock.
var ErrWouldDeadlock = errors.New("filelock: upgrade would deadlock")

// Mode describes the kind of lock held on a file.
type Mode int

const (
	// ModeShared allows other shared holders but blocks exclusive holders.
	ModeShared Mode = iota
	// ModeExclusive blocks all other holders.
	ModeExclusive
)

func (m Mode) String() string {
	switch m {
	case ModeShared:
		return "shared"
	case ModeExclusive:
		return "exclusive"
	default:
		return fmt.Sprintf("Mode(%d)", int(m))
	}
}

// Lock is an advisory lock on a single file. It is safe for concurrent use
// by multiple goroutines, though the underlying flock is per-open-file, so
// two Lock values pointing at the same path do not see each other's locks
// unless they share the same *os.File.
type Lock struct {
	mu     sync.Mutex
	file   *os.File
	path   string
	mode   Mode
	held   bool
	closed bool
}

// Open opens path (creating it with mode 0o644 if necessary) and returns a
// Lock wrapping the file. The lock is not acquired; call Lock, LockContext,
// or TryLock to do that.
func Open(path string) (*Lock, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("filelock: open %q: %w", path, err)
	}
	return &Lock{file: f, path: path}, nil
}

// Path returns the filesystem path backing this lock.
func (l *Lock) Path() string {
	return l.path
}

// File returns the underlying *os.File. The caller must not close it while
// the Lock is in use.
func (l *Lock) File() *os.File {
	return l.file
}

// Mode reports the mode of the currently held lock. It returns ModeShared
// and false when no lock is held.
func (l *Lock) Mode() (Mode, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.held {
		return ModeShared, false
	}
	return l.mode, true
}

// Held reports whether a lock is currently held.
func (l *Lock) Held() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.held
}

// Close releases any held lock and closes the underlying file. It is safe to
// call Close multiple times.
func (l *Lock) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	var unlockErr error
	if l.held {
		unlockErr = flockOp(l.file, syscall.LOCK_UN)
		l.held = false
	}
	closeErr := l.file.Close()
	if unlockErr != nil {
		return unlockErr
	}
	return closeErr
}

// Lock acquires a lock in the given mode, blocking until it succeeds or the
// context is cancelled. A nil context is treated as context.Background.
func (l *Lock) Lock(ctx context.Context, mode Mode) error {
	if ctx == nil {
		ctx = context.Background()
	}
	return l.lockSlow(ctx, mode, 0)
}

// LockContext is an alias for Lock kept for readability at call sites that
// want to emphasise the context argument.
func (l *Lock) LockContext(ctx context.Context, mode Mode) error {
	return l.Lock(ctx, mode)
}

// TryLock attempts to acquire the lock without blocking. It returns
// ErrTimeout if the lock is currently held by another process.
func (l *Lock) TryLock(mode Mode) error {
	return l.lockSlow(context.Background(), mode, -1)
}

// LockTimeout is like Lock but fails with ErrTimeout if the lock cannot be
// acquired within d. A zero or negative d means "try once, no waiting".
func (l *Lock) LockTimeout(ctx context.Context, mode Mode, d time.Duration) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if d <= 0 {
		return l.lockSlow(ctx, mode, -1)
	}
	return l.lockSlow(ctx, mode, d)
}

// lockSlow is the shared implementation. wait < 0 means non-blocking,
// wait == 0 means block indefinitely (until ctx is done), wait > 0 means
// block up to wait.
func (l *Lock) lockSlow(ctx context.Context, mode Mode, wait time.Duration) error {
	if mode != ModeShared && mode != ModeExclusive {
		return fmt.Errorf("filelock: invalid mode %d", int(mode))
	}
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return ErrClosed
	}
	if l.held {
		if l.mode == mode {
			l.mu.Unlock()
			return nil
		}
		l.mu.Unlock()
		return fmt.Errorf("filelock: already held in %s mode", l.mode)
	}
	l.mu.Unlock()

	flag := syscall.LOCK_SH
	if mode == ModeExclusive {
		flag = syscall.LOCK_EX
	}

	if wait < 0 {
		if err := flockOp(l.file, flag|syscall.LOCK_NB); err != nil {
			return ErrTimeout
		}
		return l.markHeld(mode)
	}

	deadline := time.Time{}
	if wait > 0 {
		deadline = time.Now().Add(wait)
	}

	for {
		if err := flockOp(l.file, flag|syscall.LOCK_NB); err == nil {
			return l.markHeld(mode)
		}

		if !deadline.IsZero() && !time.Now().Before(deadline) {
			return ErrTimeout
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Poll interval. flock has no portable timed wait, so we poll.
		// 5ms is short enough to be responsive and long enough not to
		// spin a CPU when many processes contend.
		sleep := 5 * time.Millisecond
		if !deadline.IsZero() {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				return ErrTimeout
			}
			if remaining < sleep {
				sleep = remaining
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleep):
		}
	}
}

func (l *Lock) markHeld(mode Mode) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		// File was closed while we were waiting; release immediately.
		_ = flockOp(l.file, syscall.LOCK_UN)
		return ErrClosed
	}
	l.held = true
	l.mode = mode
	return nil
}

// Unlock releases the currently held lock. It is a no-op if no lock is held.
func (l *Lock) Unlock() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	if !l.held {
		return nil
	}
	if err := flockOp(l.file, syscall.LOCK_UN); err != nil {
		return err
	}
	l.held = false
	return nil
}

// Upgrade attempts to convert a shared lock into an exclusive lock. It
// returns ErrWouldDeadlock if the upgrade cannot be done safely.
//
// flock upgrades are not atomic: releasing the shared lock and re-acquiring
// exclusively opens a window where another process may grab the exclusive
// lock, and two upgraders can deadlock each other. To stay deadlock-free we
// refuse the upgrade and let the caller decide whether to release and retry.
func (l *Lock) Upgrade() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	if !l.held {
		return fmt.Errorf("filelock: no lock held to upgrade")
	}
	if l.mode == ModeExclusive {
		return nil
	}
	return ErrWouldDeadlock
}

// TryUpgrade attempts to upgrade a shared lock to exclusive without blocking.
// It returns ErrWouldDeadlock if the upgrade cannot be performed atomically.
// On success the lock mode becomes ModeExclusive.
//
// The only safe case is when no other process holds a shared lock. We probe
// by attempting a non-blocking exclusive flock; on Linux this succeeds only
// if we are the sole holder. If it fails we drop back to the shared lock.
func (l *Lock) TryUpgrade() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	if !l.held {
		return fmt.Errorf("filelock: no lock held to upgrade")
	}
	if l.mode == ModeExclusive {
		return nil
	}

	// On Linux, flock(LOCK_EX|LOCK_NB) on a fd that already holds LOCK_SH
	// atomically converts to exclusive if possible; otherwise it fails
	// with EWOULDBLOCK and the shared lock remains held. That is exactly
	// the primitive we need, so no release/re-acquire dance is required.
	if err := flockOp(l.file, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return ErrWouldDeadlock
	}
	l.mode = ModeExclusive
	return nil
}

// Downgrade converts an exclusive lock into a shared lock. It is atomic on
// Linux and always succeeds while the lock is held.
func (l *Lock) Downgrade() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	if !l.held {
		return fmt.Errorf("filelock: no lock held to downgrade")
	}
	if l.mode == ModeShared {
		return nil
	}
	if err := flockOp(l.file, syscall.LOCK_SH|syscall.LOCK_NB); err != nil {
		return err
	}
	l.mode = ModeShared
	return nil
}

// flockOp applies a flock operation to f. It is split out so tests can
// substitute behaviour if needed and so the rest of the code reads cleanly.
func flockOp(f *os.File, how int) error {
	return syscall.Flock(int(f.Fd()), how)
}
