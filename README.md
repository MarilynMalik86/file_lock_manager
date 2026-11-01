# file_lock_manager

Advisory file locking via `flock(2)` with context cancellation, timeouts, and
non-blocking upgrade attempts.

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	filelock "file_lock_manager"
)

func main() {
	l, err := filelock.Open("/tmp/myapp.lock")
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := l.Lock(ctx, filelock.ModeExclusive); err != nil {
		log.Fatal(err)
	}
	defer l.Unlock()

	fmt.Println("holding exclusive lock on", l.Path())
}
```

## Why this exists

The standard library has no cross-process advisory locking. `syscall.Flock`
is available, but it blocks forever, ignores `context.Context`, and its
upgrade semantics are a trap. This package wraps `flock` in a small `Lock`
type that:

- takes a `context.Context` so a wait can be cancelled,
- offers `LockTimeout` for callers who want a bounded wait,
- refuses to perform a blocking shared-to-exclusive upgrade, because two
  processes doing that at once can deadlock,
- exposes `TryUpgrade` for the case where an atomic non-blocking upgrade is
  actually safe.

The trade-off: `flock` has no portable timed wait on any platform, so the
blocking paths poll with a 5 ms sleep. That is cheap enough for lock files,
but it is not a general-purpose high-frequency synchronisation primitive.

## Edge you will hit

`flock` locks are per open file description, not per path. Two `Lock` values
opened on the same path in the same process are independent from the kernel's
point of view only when they use distinct file descriptors; two `Lock` values
that share a descriptor (for example after `dup`) share the lock. If you need
two independent locks in one process, call `Open` twice.

## API

- `Open(path string) (*Lock, error)` — opens or creates the file and returns
  a `Lock` that is not yet locked.
- `(*Lock) Lock(ctx context.Context, mode Mode) error` — blocks until the
  lock is acquired or `ctx` is done.
- `(*Lock) LockContext(ctx context.Context, mode Mode) error` — alias for
  `Lock`.
- `(*Lock) LockTimeout(ctx context.Context, mode Mode, d time.Duration) error`
  — blocks at most `d`; returns `ErrTimeout` on expiry.
- `(*Lock) TryLock(mode Mode) error` — non-blocking; returns `ErrTimeout`
  when contended.
- `(*Lock) Unlock() error` — releases a held lock.
- `(*Lock) Upgrade() error` — always returns `ErrWouldDeadlock` for a shared
  lock; a no-op for an exclusive lock.
- `(*Lock) TryUpgrade() error` — atomically upgrades a shared lock to
  exclusive when this process is the only holder; otherwise
  `ErrWouldDeadlock`.
- `(*Lock) Downgrade() error` — atomically converts exclusive to shared.
- `(*Lock) Held() bool`, `(*Lock) Mode() (Mode, bool)`, `(*Lock) Path() string`,
  `(*Lock) File() *os.File`.
- `(*Lock) Close() error` — releases and closes; safe to call more than once.

Modes are `ModeShared` and `ModeExclusive`. Errors are `ErrClosed`,
`ErrTimeout`, and `ErrWouldDeadlock`; context cancellation surfaces as the
context's own error.
