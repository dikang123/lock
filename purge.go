package lock

import (
	"gopkg.in/mgo.v2"
)

// A Purger purges expired locks.
type Purger interface {
	Purge() error
}

// purge implements the Purger interface.
type purger struct {
	coll   *mgo.Collection
	locker Locker
}

// NewPurger creates a new Purger.
func NewPurger(collection *mgo.Collection) Purger {
	return &purger{
		coll:   collection,
		locker: NewMongoLocker(collection),
	}
}

// Purge purges expired locks. It is important to note that not just locks with
// a TTL of 0 get purged - any lock that sharess a lockId with a lock that has
// has a TTL of zero will be removed. Basically all locks with the same lockId
// will be purged whenever the first one expires.
func (p *purger) Purge() error { // !!!!!!!!!!!!!! return locks unlocked
	filter := Filter{
		TTLlt: 1,
	}
	locks, err := p.locker.Status(filter)
	if err != nil {
		return err
	}

	for _, lock := range locks {
		_, err = p.locker.Unlock(lock.LockId)
		if err != nil {
			// It's fine if the lock is gone, that's what we want.
			if err == ErrLockNotFound {
				continue
			}
			return err
		}
	}

	return nil
}
