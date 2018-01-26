// Package lock provides an interface for creating exclusive and shared locks
// in MongoDB.
package lock

import (
	"errors"
	"sort"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const (
	// Lock type strings.
	LOCK_TYPE_EXCLUSIVE = "exclusive"
	LOCK_TYPE_SHARED    = "shared"
)

var (
	ErrAlreadyLocked = errors.New("unable to acquire lock (resource is already locked)")
	ErrLockNotFound  = errors.New("unable to find lock")
)

// LockDetails contains fields that are used when creating a lock.
type LockDetails struct {
	// The user that is creating the lock.
	Owner string
	// The host that the lock is being created from.
	Host string
	// The time to live (TTL) for the lock, in seconds. Setting this to 0
	// means that the lock will not have a TTL.
	TTL uint
}

// LockStatus represents the status of a lock.
type LockStatus struct {
	// The name of the resource that the lock is on.
	Resource string
	// The id of the lock.
	LockId string
	// The type of the lock ("exclusive" or "shared")
	Type string
	// The name of the user who created the lock.
	Owner string
	// The host that the lock was created from.
	Host string
	// The time that the lock was created at.
	CreatedAt time.Time
	// The time that the lock was renewed at, if applicable.
	RenewedAt *time.Time
	// The TTL for the lock, in seconds. A negative value means that the
	// lock does not have a TTL.
	TTL int64
}

// LockStatusesByCreatedAtDesc is a slice of LockStatus structs, ordered by
// CreatedAt descending.
type LockStatusesByCreatedAtDesc []LockStatus

// Filter contains fields that are used to filter locks when querying their
// status. Fields with zero values are ignored.
type Filter struct {
	CreatedBefore time.Time // Only include locks created before this time.
	CreatedAfter  time.Time // Only include locks created after this time.
	TTLlt         uint      // Only include locks with a TTL less than this value, in seconds.
	TTLgt         uint      // Only include locks with a TTL greater than this value, in seconds.
	Resource      string    // Only include locks on this resource.
	LockId        string    // Only include locks with this lockId.
	Owner         string    // Only include locks with this owner.
}

// lock represents a lock object stored in Mongo.
type lock struct {
	// Use pointers so we can store null values in the db.
	LockId    *string    `bson:"lockId"`
	Owner     *string    `bson:"owner"`
	Host      *string    `bson:"host"`
	CreatedAt *time.Time `bson:"createdAt"`
	RenewedAt *time.Time `bson:"renewedAt"`
	ExpiresAt *time.Time `bson:"expiresAt"` // How TTLs are stored internally.
	Acquired  bool       `bson:"acquired"`
}

// sharedLocks represents a slice of shared locks stored in Mongo.
type sharedLocks struct {
	Count uint   `bson:"count"`
	Locks []lock `bson:"locks"`
}

// resource represents a resource object stored in Mongo.
type resource struct {
	Name      string      `bson:"resource"`
	Exclusive lock        `bson:"exclusive"`
	Shared    sharedLocks `bson:"shared"`
}

// A Locker manages exclusive and shared locks on resources. It requires a
// resource name (the object that gets locked) and a lockId (lock identifier)
// when creating a lock. Clients can create multiple locks with the same
// lockId, making it easy to unlock or renew a group of related locks at the
// same time. Another reason for using lockIds is to ensure that only the
// client that creates a lock knows the lockId needed to unlock it - knowing a
// resource name alone is not enough to unlock it.
type Locker interface {
	// XLock creates an exclusive lock on a resource and associates it with
	// the provided lockId. Additional details about the lock can also be
	// supplied via LockDetails.
	XLock(resource, lockId string, ld LockDetails) error

	// SLock creates a shared lock on a resource and associates it with the
	// provided lockId. Additional details about the lock can also be
	// supplied via LockDetails.
	//
	// The maxConcurrent argument is used to limit the number of shared
	// locks that can exist on a resource concurrently. When a client calls
	// SLock with maxCurrent = N, the lock request will fail unless there
	// are less than N shared locks on the resource already. If
	// maxConcurrent is negative then there is no limit to the number of
	// shared locks that can exist.
	SLock(resource, lockId string, ld LockDetails, maxConcurrent int) error

	// Unlock unlocks all resources with the associated lockId. If there
	// are multiple locks with the given lockId, they will be unlocked in
	// the reverse order in which they were created in (newest to oldest).
	// For every lock that is unlocked, a LockStatus struct (representing
	// the lock before it was unlocked) is returned. The order of these is
	// the order in which they were unlocked.
	//
	// If an error is returned, it is safe and recommended for clients to
	// retry until there is no error.
	Unlock(lockId string) ([]LockStatus, error)

	// Status returns the status of locks that match the provided filter.
	// Fields with zero values in the Filter struct are ignored.
	Status(f Filter) ([]LockStatus, error)

	// Renew updates the TTL of all locks with the associated lockId. The
	// new TTL for the locks will be the value of the argument provided.
	//
	// A LockStatus struct is returned for each lock that is renewed. The
	// TTL field in each struct represents the new TTL. A client can use
	// this information to ensure that all of the locks they created with
	// the lockId have been renewed. If not all locks have been renewed,
	// or if an error is returned, clients should assume that none of the
	// locks are safe to use, and they should unlock the lockId.
	Renew(lockId string, ttl uint) ([]LockStatus, error)
}

// mongoLocker implements the Locker interface with MongoDB.
type mongoLocker struct {
	coll *mgo.Collection
}

// NewMongoLocker creates a Locker that is backed by MongoDB.
func NewMongoLocker(collection *mgo.Collection) Locker {
	return &mongoLocker{
		coll: collection,
	}
}

func (l *mongoLocker) XLock(resourceName, lockId string, ld LockDetails) error {
	selector := bson.M{
		"resource":           resourceName,
		"exclusive.acquired": false,
		"shared.count":       0,
	}

	r := &resource{
		Name:      resourceName,
		Exclusive: lockFromDetails(lockId, ld),
		Shared: sharedLocks{
			Count: 0,
			Locks: []lock{},
		},
	}

	change := mgo.Change{
		Update: r,
		Upsert: true,
	}

	// One of three things will happen when we run this change (upsert).
	// 1) If the resource exists and has any locks on it (shared or
	//    exclusive), we will get a duplicate key error.
	// 2) If the resource exists but doesn't have any locks on it, we will
	//    update it to obtain an exclusive lock.
	// 3) If the resource doesn't exist yet, it will be inserted which will
	//    give us an exclusive lock on it.
	_, err := l.coll.Find(selector).Apply(change, map[string]interface{}{})
	if err != nil {
		if mgo.IsDup(err) {
			return ErrAlreadyLocked
		}
		return err
	}

	// Acquired lock.
	return nil
}

func (l *mongoLocker) SLock(resourceName, lockId string, ld LockDetails, maxConcurrent int) error {
	selector := bson.M{
		"resource":           resourceName,
		"exclusive.acquired": false,
		"shared.locks.lockId": bson.M{
			"$ne": lockId,
		},
	}
	if maxConcurrent >= 0 {
		selector["shared.count"] = bson.M{
			"$lt": maxConcurrent,
		}
	}

	change := mgo.Change{
		Update: bson.M{
			"$inc": bson.M{
				"shared.count": 1,
			},
			"$push": bson.M{
				"shared.locks": lockFromDetails(lockId, ld),
			},
		},
		Upsert: true,
	}

	// One of three things will happen when we run this change (upsert).
	// 1) If the resource exists and has an exclusive lock on it, or if it
	//    exists and has a shared lock on it with the same lockId, we will
	//    get a duplicate key error.
	// 2) If the resource exists but doesn't have an exclusive lock or a
	//    a shared lock with the same lockId on it, we will update it to
	//    obtain a shared lock so long as there are less than maxConcurrent
	//    shared locks on it already.
	// 3) If the resource doesn't exist yet, it will be inserted which will
	//    give us a shared lock on it.
	_, err := l.coll.Find(selector).Apply(change, map[string]interface{}{})
	if err != nil {
		if mgo.IsDup(err) {
			return ErrAlreadyLocked
		}
		return err
	}

	// Acquired lock.
	return nil
}

func (l *mongoLocker) Unlock(lockId string) ([]LockStatus, error) {
	// First find the locks associated with the lockId.
	filter := Filter{
		LockId: lockId,
	}
	locks, err := l.Status(filter)
	if err != nil {
		return []LockStatus{}, err
	}

	// Sort the lock statuses by most recent CreatedAt date first.
	var sortedLocks LockStatusesByCreatedAtDesc
	sortedLocks = locks
	sort.Sort(sortedLocks)

	unlocked := []LockStatus{}
	for _, lock := range sortedLocks {
		var err error
		switch lock.Type {
		case LOCK_TYPE_EXCLUSIVE:
			err = l.xUnlock(lock.Resource, lock.LockId)
		case LOCK_TYPE_SHARED:
			err = l.sUnlock(lock.Resource, lock.LockId)
		}
		if err != nil {
			if err == ErrLockNotFound {
				// It's fine if the lock is gone, that's
				// what we want.
				continue
			}
			return unlocked, err
		}
		unlocked = append(unlocked, lock)
	}

	// Unlocks were successful.
	return unlocked, nil
}

func (l *mongoLocker) Status(f Filter) ([]LockStatus, error) {
	// Construct two queries, one for exclusive locks and one for shared
	// locks, which will get combined with the "OR" operator before we send
	// the final query to Mongo.
	xQuery := bson.M{}
	sQuery := bson.M{}

	// Create an object that will be used to filter out unmatched shared
	// locks from the results set we will get back from Mongo.
	filterCond := []bson.M{}

	if !f.CreatedBefore.IsZero() {
		xQuery["exclusive.createdAt"] = bson.M{
			"$lt": f.CreatedBefore,
		}
		sQuery["shared.locks.createdAt"] = bson.M{
			"$lt": f.CreatedBefore,
		}
		filterCond = append(filterCond, bson.M{
			"$lt": []interface{}{"$$lock.createdAt", f.CreatedBefore},
		})
	}
	if !f.CreatedAfter.IsZero() {
		xQuery["exclusive.createdAt"] = bson.M{
			"$gt": f.CreatedAfter,
		}
		sQuery["shared.locks.createdAt"] = bson.M{
			"$gt": f.CreatedAfter,
		}
		filterCond = append(filterCond, bson.M{
			"$gt": []interface{}{"$$lock.createdAt", f.CreatedAfter},
		})
	}

	if f.TTLlt > 0 {
		ttlTime := time.Now().Add(time.Duration(f.TTLlt) * time.Second)
		xQuery["exclusive.expiresAt"] = bson.M{
			"$lt": ttlTime,
		}
		sQuery["shared.locks.expiresAt"] = bson.M{
			"$lt": ttlTime,
		}
		filterCond = append(filterCond, bson.M{
			"$lt": []interface{}{"$$lock.expiresAt", ttlTime},
		})
	}
	if f.TTLgt > 0 {
		ttlTime := time.Now().Add(time.Duration(f.TTLgt) * time.Second)
		xQuery["exclusive.expiresAt"] = bson.M{
			"$gt": ttlTime,
		}
		sQuery["shared.locks.expiresAt"] = bson.M{
			"$gt": ttlTime,
		}
		filterCond = append(filterCond, bson.M{
			"$gt": []interface{}{"$$lock.expiresAt", ttlTime},
		})
	}

	if f.LockId != "" {
		xQuery["exclusive.lockId"] = f.LockId
		sQuery["shared.locks.lockId"] = f.LockId
		filterCond = append(filterCond, bson.M{
			"$eq": []interface{}{"$$lock.lockId", f.LockId},
		})
	}

	if f.Owner != "" {
		xQuery["exclusive.owner"] = f.Owner
		sQuery["shared.locks.owner"] = f.Owner
		filterCond = append(filterCond, bson.M{
			"$eq": []interface{}{"$$lock.owner", f.Owner},
		})
	}

	// Construct the part of the query that will be used to match locks that
	// satisfy the provided filters.
	match := bson.M{
		"$or": []bson.M{xQuery, sQuery},
	}
	if f.Resource != "" {
		match["resource"] = f.Resource
	}

	// Construct the part of the query that will remove locks that don't
	// satisfy the provided filters from the results set. This is needed
	// because, by default, Mongo will return an entire document even if
	// only part of it matches the search query. So if a resource has 10
	// shared locks on it and only one of them satisfies the filter, we
	// want to remove all of the other 9 locks from the results we get back.
	project := bson.M{
		"shared.locks": bson.M{
			"$filter": bson.M{
				"input": "$shared.locks",
				"as":    "lock",
				"cond": bson.M{
					"$and": filterCond,
				},
			},
		},
		"resource":  true,
		"exclusive": true,
	}

	// Construct the final query.
	query := []bson.M{
		bson.M{
			"$match": match,
		},
		bson.M{
			"$project": project,
		},
	}

	resources := []resource{}
	err := l.coll.Pipe(query).All(&resources)
	if err != nil {
		return []LockStatus{}, err
	}

	// Convert the results set to a []LockStatus.
	statuses := []LockStatus{}
	for _, r := range resources {
		if r.Exclusive.Acquired {
			ls := statusFromLock(r.Exclusive)
			ls.Resource = r.Name
			ls.Type = LOCK_TYPE_EXCLUSIVE

			statuses = append(statuses, ls)
		}

		for _, shared := range r.Shared.Locks {
			ls := statusFromLock(shared)
			ls.Resource = r.Name
			ls.Type = LOCK_TYPE_SHARED

			statuses = append(statuses, ls)
		}
	}

	return statuses, nil
}

func (l *mongoLocker) Renew(lockId string, ttl uint) ([]LockStatus, error) {
	// Retrieve all of the locks with the given lockId.
	filter := Filter{
		LockId: lockId,
	}
	locks, err := l.Status(filter)
	if err != nil {
		return []LockStatus{}, err
	}

	// Return an error if no locks were found.
	if len(locks) == 0 {
		return []LockStatus{}, ErrLockNotFound
	}

	renewedAt := time.Now()
	newExpiration := renewedAt.Add(time.Duration(ttl) * time.Second)

	// Update the expiration date for each lock.
	statuses := []LockStatus{}
	selector := bson.M{}
	change := mgo.Change{}
	for _, lock := range locks {
		if lock.Type == LOCK_TYPE_EXCLUSIVE {
			selector = bson.M{
				"resource":         lock.Resource,
				"exclusive.lockId": lock.LockId,
			}

			change = mgo.Change{
				Update: bson.M{
					"$set": bson.M{
						"exclusive.expiresAt": newExpiration,
						"exclusive.renewedAt": renewedAt,
					},
				},
			}
		}

		if lock.Type == LOCK_TYPE_SHARED {
			selector = bson.M{
				"resource":            lock.Resource,
				"shared.locks.lockId": lock.LockId,
			}

			change = mgo.Change{
				Update: bson.M{
					"$set": bson.M{
						"shared.locks.$.expiresAt": newExpiration,
						"shared.locks.$.renewedAt": renewedAt,
					},
				},
			}
		}

		// One of two things will happen when we run this change.
		// 1) If the lock doesn't exist, ErrLockNotFound will be returned.
		// 2) If the lock exists, it's TTL will be updated.
		_, err := l.coll.Find(selector).Apply(change, map[string]interface{}{})
		if err != nil {
			if err == mgo.ErrNotFound {
				return statuses, ErrLockNotFound
			}
			return statuses, err
		}

		lock.TTL = calcTTL(&newExpiration)
		lock.RenewedAt = &renewedAt
		statuses = append(statuses, lock)
	}

	return statuses, nil
}

// ------------------------------------------------------------------------- //

// xUnlock unlocks an exclusive lock on a resource.
func (l *mongoLocker) xUnlock(resourceName, lockId string) error {
	selector := bson.M{
		"resource":         resourceName,
		"exclusive.lockId": lockId,
	}

	change := mgo.Change{
		Update: bson.M{
			"$set": bson.M{
				"exclusive": &lock{},
			},
		},
	}

	// One of two things will happen when we run this change.
	// 1) If the resource doesn't exist, or it exists but doesn't have an
	//    exclusive lock with the given lockId on it, ErrLockNotFound will
	//    be returned.
	// 2) If the resource exists and has an exclusive lock with the given
	//    lockId on it, the exclusive lock will be removed from the resource.
	_, err := l.coll.Find(selector).Apply(change, map[string]interface{}{})
	if err != nil {
		if err == mgo.ErrNotFound {
			return ErrLockNotFound
		}
		return err
	}

	// Unlocked lock.
	return nil
}

// xUnlock unlocks a shared lock on a resource.
func (l *mongoLocker) sUnlock(resourceName, lockId string) error {
	selector := bson.M{
		"resource":            resourceName,
		"shared.locks.lockId": lockId,
	}

	change := mgo.Change{
		Update: bson.M{
			"$inc": bson.M{
				"shared.count": -1,
			},
			"$pull": bson.M{
				"shared.locks": bson.M{
					"lockId": lockId,
				},
			},
		},
	}

	// One of two things will happen when we run this change.
	// 1) If the resource doesn't exist, or it exists but doesn't have a
	//    shared lock with the given lockId on it, ErrLockNotFound will be
	//    returned.
	// 2) If the resource exists and has a shared lock with the given lockId
	//    on it, the shared lock corresponding to the lockId will be removed
	//    from the resource.
	_, err := l.coll.Find(selector).Apply(change, map[string]interface{}{})
	if err != nil {
		if err == mgo.ErrNotFound {
			return ErrLockNotFound
		}
		return err
	}

	// Unlocked lock.
	return nil
}

// lockFromDetails creates a lock struct from a lockId and a LockDetails struct.
func lockFromDetails(lockId string, ld LockDetails) lock {
	now := time.Now()

	lock := lock{
		LockId:    &lockId,
		CreatedAt: &now,
		Acquired:  true,
	}

	if ld.Owner != "" {
		lock.Owner = &ld.Owner
	}
	if ld.Host != "" {
		lock.Host = &ld.Host
	}
	if ld.TTL > 0 {
		e := now.Add(time.Duration(ld.TTL) * time.Second)
		lock.ExpiresAt = &e
	}

	return lock
}

// statusFromLock creates a LockStatus struct from a lock struct.
func statusFromLock(l lock) LockStatus {
	ls := LockStatus{
		RenewedAt: l.RenewedAt,
	}

	// Be careful and make sure we don't dereference a nil pointer.
	if l.LockId != nil {
		ls.LockId = *l.LockId
	}
	if l.Owner != nil {
		ls.Owner = *l.Owner
	}
	if l.Host != nil {
		ls.Host = *l.Host
	}
	if l.CreatedAt != nil {
		ls.CreatedAt = *l.CreatedAt
	}
	if l.ExpiresAt != nil {
		ls.TTL = calcTTL(l.ExpiresAt)
	}

	return ls
}

// calcTTL calculates a TTL value, in seconds, by subtracting the current time
// from an expiration time. If the expiration time has already past, it returns
// 0. If there is no expiration time, it returns -1.
func calcTTL(expiresAt *time.Time) int64 {
	if expiresAt == nil {
		// There is no TTL.
		return -1
	}

	delta := expiresAt.Sub(time.Now())
	ttl := int64(delta.Seconds()) // Don't need sub-second granularity.
	if ttl < 0 {
		return 0
	}
	return ttl
}

func (ls LockStatusesByCreatedAtDesc) Len() int { return len(ls) }
func (ls LockStatusesByCreatedAtDesc) Less(i, j int) bool {
	return ls[i].CreatedAt.After(ls[j].CreatedAt)
}
func (ls LockStatusesByCreatedAtDesc) Swap(i, j int) { ls[i], ls[j] = ls[j], ls[i] }
