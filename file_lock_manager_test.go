package filelock

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func tempPath(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	return filepath.Join(dir, "lockfile")
}

func TestOpenCreatesFile(t *testing.T) {
	path := tempPath(t)
	l, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l.Close()

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected file to exist: %v", err)
	}
	if l.Path() != path {
		t.Fatalf("Path() = %q, want %q", l.Path(), path)
	}
}

func TestLockExclusiveAndUnlock(t *testing.T) {
	path := tempPath(t)
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	if err := l.Lock(context.Background(), ModeExclusive); err != nil {
		t.Fatalf("Lock: %v", err)
	}
	if !l.Held() {
		t.Fatal("expected lock to be held")
	}
	mode, ok := l.Mode()
	if !ok || mode != ModeExclusive {
		t.Fatalf("Mode() = (%v, %v), want (exclusive, true)", mode, ok)
	}

	if err := l.Unlock(); err != nil {
		t.Fatalf("Unlock: %v", err)
	}
	if l.Held() {
		t.Fatal("expected lock to be released")
	}
	if _, ok := l.Mode(); ok {
		t.Fatal("Mode() reported a held lock after Unlock")
	}
}

func TestLockIsIdempotentForSameMode(t *testing.T) {
	path := tempPath(t)
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	if err := l.Lock(context.Background(), ModeShared); err != nil {
		t.Fatal(err)
	}
	if err := l.Lock(context.Background(), ModeShared); err != nil {
		t.Fatalf("second Lock with same mode: %v", err)
	}
}

func TestLockRejectsConflictingMode(t *testing.T) {
	path := tempPath(t)
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	if err := l.Lock(context.Background(), ModeShared); err != nil {
		t.Fatal(err)
	}
	if err := l.Lock(context.Background(), ModeExclusive); err == nil {
		t.Fatal("expected error when re-locking with different mode")
	}
}

func TestTryLockContention(t *testing.T) {
	path := tempPath(t)
	a, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	if err := a.Lock(context.Background(), ModeExclusive); err != nil {
		t.Fatal(err)
	}
	if err := b.TryLock(ModeExclusive); !errors.Is(err, ErrTimeout) {
		t.Fatalf("TryLock = %v, want ErrTimeout", err)
	}
}

func TestTryLockSharedCompatible(t *testing.T) {
	path := tempPath(t)
	a, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	if err := a.Lock(context.Background(), ModeShared); err != nil {
		t.Fatal(err)
	}
	if err := b.TryLock(ModeShared); err != nil {
		t.Fatalf("two shared locks should coexist: %v", err)
	}
}

func TestLockContextCancelled(t *testing.T) {
	path := tempPath(t)
	a, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	if err := a.Lock(context.Background(), ModeExclusive); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := b.Lock(ctx, ModeExclusive); !errors.Is(err, context.Canceled) {
		t.Fatalf("Lock with cancelled ctx = %v, want context.Canceled", err)
	}
}

func TestLockContextDeadline(t *testing.T) {
	path := tempPath(t)
	a, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	if err := a.Lock(context.Background(), ModeExclusive); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	err = b.Lock(ctx, ModeExclusive)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Lock with expired deadline = %v, want context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("Lock took %v, expected to return promptly", elapsed)
	}
}

func TestLockTimeoutReturnsErrTimeout(t *testing.T) {
	path := tempPath(t)
	a, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	if err := a.Lock(context.Background(), ModeExclusive); err != nil {
		t.Fatal(err)
	}

	err = b.LockTimeout(context.Background(), ModeExclusive, 30*time.Millisecond)
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("LockTimeout = %v, want ErrTimeout", err)
	}
}

func TestLockTimeoutZeroIsNonBlocking(t *testing.T) {
	path := tempPath(t)
	a, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	if err := a.Lock(context.Background(), ModeExclusive); err != nil {
		t.Fatal(err)
	}
	if err := b.LockTimeout(context.Background(), ModeExclusive, 0); !errors.Is(err, ErrTimeout) {
		t.Fatalf("LockTimeout(0) = %v, want ErrTimeout", err)
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	path := tempPath(t)
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestOperationsAfterClose(t *testing.T) {
	path := tempPath(t)
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	if err := l.Lock(context.Background(), ModeExclusive); !errors.Is(err, ErrClosed) {
		t.Fatalf("Lock after Close = %v, want ErrClosed", err)
	}
	if err := l.Unlock(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Unlock after Close = %v, want ErrClosed", err)
	}
	if err := l.Upgrade(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Upgrade after Close = %v, want ErrClosed", err)
	}
}

func TestUpgradeRefusesWhenShared(t *testing.T) {
	path := tempPath(t)
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	if err := l.Lock(context.Background(), ModeShared); err != nil {
		t.Fatal(err)
	}
	if err := l.Upgrade(); !errors.Is(err, ErrWouldDeadlock) {
		t.Fatalf("Upgrade = %v, want ErrWouldDeadlock", err)
	}
	// The shared lock must still be held after a refused upgrade.
	if !l.Held() {
		t.Fatal("lock should still be held after refused upgrade")
	}
	mode, _ := l.Mode()
	if mode != ModeShared {
		t.Fatalf("mode after refused upgrade = %v, want shared", mode)
	}
}

func TestUpgradeNoOpWhenExclusive(t *testing.T) {
	path := tempPath(t)
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	if err := l.Lock(context.Background(), ModeExclusive); err != nil {
		t.Fatal(err)
	}
	if err := l.Upgrade(); err != nil {
		t.Fatalf("Upgrade on exclusive lock = %v, want nil", err)
	}
}

func TestUpgradeWithoutLock(t *testing.T) {
	path := tempPath(t)
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	if err := l.Upgrade(); err == nil {
		t.Fatal("Upgrade without a held lock should fail")
	}
}

func TestTryUpgradeSucceedsWhenSoleHolder(t *testing.T) {
	path := tempPath(t)
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	if err := l.Lock(context.Background(), ModeShared); err != nil {
		t.Fatal(err)
	}
	if err := l.TryUpgrade(); err != nil {
		t.Fatalf("TryUpgrade as sole holder: %v", err)
	}
	mode, ok := l.Mode()
	if !ok || mode != ModeExclusive {
		t.Fatalf("mode after TryUpgrade = (%v, %v), want (exclusive, true)", mode, ok)
	}
}

func TestTryUpgradeFailsWithOtherSharedHolder(t *testing.T) {
	path := tempPath(t)
	a, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	if err := a.Lock(context.Background(), ModeShared); err != nil {
		t.Fatal(err)
	}
	if err := b.Lock(context.Background(), ModeShared); err != nil {
		t.Fatal(err)
	}

	if err := a.TryUpgrade(); !errors.Is(err, ErrWouldDeadlock) {
		t.Fatalf("TryUpgrade with competing holder = %v, want ErrWouldDeadlock", err)
	}
	// Both locks must still be held in shared mode.
	if mode, _ := a.Mode(); mode != ModeShared {
		t.Fatalf("a mode = %v, want shared", mode)
	}
	if mode, _ := b.Mode(); mode != ModeShared {
		t.Fatalf("b mode = %v, want shared", mode)
	}
}

func TestDowngrade(t *testing.T) {
	path := tempPath(t)
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	if err := l.Lock(context.Background(), ModeExclusive); err != nil {
		t.Fatal(err)
	}
	if err := l.Downgrade(); err != nil {
		t.Fatalf("Downgrade: %v", err)
	}
	mode, ok := l.Mode()
	if !ok || mode != ModeShared {
		t.Fatalf("mode after Downgrade = (%v, %v), want (shared, true)", mode, ok)
	}
}

func TestDowngradeNoOpWhenShared(t *testing.T) {
	path := tempPath(t)
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	if err := l.Lock(context.Background(), ModeShared); err != nil {
		t.Fatal(err)
	}
	if err := l.Downgrade(); err != nil {
		t.Fatalf("Downgrade on shared lock = %v, want nil", err)
	}
}

func TestDowngradeWithoutLock(t *testing.T) {
	path := tempPath(t)
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	if err := l.Downgrade(); err == nil {
		t.Fatal("Downgrade without a held lock should fail")
	}
}

func TestInvalidMode(t *testing.T) {
	path := tempPath(t)
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	if err := l.Lock(context.Background(), Mode(42)); err == nil {
		t.Fatal("Lock with invalid mode should fail")
	}
}

func TestNilContextTreatedAsBackground(t *testing.T) {
	path := tempPath(t)
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	//nolint:staticcheck // deliberately passing nil to verify the guard
	if err := l.Lock(nil, ModeExclusive); err != nil {
		t.Fatalf("Lock(nil) = %v, want nil", err)
	}
}

func TestConcurrentUnlockAndLock(t *testing.T) {
	path := tempPath(t)
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				if err := l.Lock(context.Background(), ModeShared); err != nil {
					return
				}
				_ = l.Unlock()
			}
		}()
	}
	wg.Wait()
}

func TestModeString(t *testing.T) {
	if got := ModeShared.String(); got != "shared" {
		t.Fatalf("ModeShared.String() = %q", got)
	}
	if got := ModeExclusive.String(); got != "exclusive" {
		t.Fatalf("ModeExclusive.String() = %q", got)
	}
	if got := Mode(7).String(); got != "Mode(7)" {
		t.Fatalf("Mode(7).String() = %q", got)
	}
}
