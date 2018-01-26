// Package mock provides mock structures that satisfy interfaces in this package.
package mock

import (
	"github.com/mongo-go/lock"
)

type Locker struct {
	XLockFunc  func(resource, lockId string, ld lock.LockDetails) error
	SLockFunc  func(resource, lockId string, ld lock.LockDetails, maxConcurrent int) error
	UnlockFunc func(lockId string) ([]lock.LockStatus, error)
	StatusFunc func(f lock.Filter) ([]lock.LockStatus, error)
	RenewFunc  func(lockId string, ttl uint) ([]lock.LockStatus, error)
}

func (l *Locker) XLock(resource, lockId string, ld lock.LockDetails) error {
	if l.XLockFunc != nil {
		return l.XLockFunc(resource, lockId, ld)
	}
	return nil
}

func (l *Locker) SLock(resource, lockId string, ld lock.LockDetails, maxConcurrent int) error {
	if l.SLockFunc != nil {
		return l.SLockFunc(resource, lockId, ld, maxConcurrent)
	}
	return nil
}

func (l *Locker) Unlock(lockId string) ([]lock.LockStatus, error) {
	if l.UnlockFunc != nil {
		return l.UnlockFunc(lockId)
	}
	return []lock.LockStatus{}, nil
}

func (l *Locker) Status(f lock.Filter) ([]lock.LockStatus, error) {
	if l.StatusFunc != nil {
		return l.StatusFunc(f)
	}
	return []lock.LockStatus{}, nil
}

func (l *Locker) Renew(lockId string, ttl uint) ([]lock.LockStatus, error) {
	if l.RenewFunc != nil {
		return l.RenewFunc(lockId, ttl)
	}
	return []lock.LockStatus{}, nil
}
