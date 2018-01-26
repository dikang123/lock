package lock_test

import (
	"fmt"
	"net"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/go-test/deep"
	"github.com/mongo-go/lock"
	"gopkg.in/mgo.v2"
)

const (
	// Environment variables that, if set, can be used to override things
	// in the tests.
	ENV_VAR_TEST_MONGO_URL        = "TEST_MONGO_URL"
	ENV_VAR_TEST_MONGO_DB         = "TEST_MONGO_DB"
	ENV_VAR_TEST_MONGO_COLLECTION = "TEST_MONGO_COLLECTION"
)

// @TODO: move this into its own package.
func setup(t *testing.T) *mgo.Collection {
	// Defaults.
	url := "localhost:3000"
	db := "test"
	collName := "locks"
	timeoutSec := time.Duration(20) * time.Second

	// Override the url, db, and collection if the proper env vars are set.
	if urlOverride := os.Getenv(ENV_VAR_TEST_MONGO_URL); urlOverride != "" {
		url = urlOverride
	}
	if dbOverride := os.Getenv(ENV_VAR_TEST_MONGO_DB); dbOverride != "" {
		db = dbOverride
	}
	if collOverride := os.Getenv(ENV_VAR_TEST_MONGO_COLLECTION); collOverride != "" {
		collName = collOverride
	}

	dialInfo, err := mgo.ParseURL(url)
	if err != nil {
		t.Errorf("error connecting to mongo on '%s': %s", url, err)
	}

	dialInfo.DialServer = func(addr *mgo.ServerAddr) (net.Conn, error) {
		conn, err := net.DialTimeout("tcp", addr.String(), timeoutSec)
		if err != nil {
			return nil, err
		}
		return conn, nil
	}
	dialInfo.Timeout = timeoutSec

	s, err := mgo.DialWithInfo(dialInfo)
	if err != nil {
		t.Errorf("error connecting to mongo on '%s': %s", url, err)
	}

	coll := s.DB(db).C(collName)

	// Add the required unique index on the 'resource' field.
	index := mgo.Index{
		Key:        []string{"resource"},
		Unique:     true,
		DropDups:   true,
		Background: false,
		Sparse:     true,
	}
	err = coll.EnsureIndex(index)
	if err != nil {
		t.Errorf("error creating index '%#v': %s", index.Key, err)
	}

	return coll
}

func teardown(t *testing.T, coll *mgo.Collection) {
	err := coll.DropCollection()
	if err != nil {
		t.Errorf("error dropping the collection '%s': %s'", coll.FullName, err)
	}
}

func TestLockExclusive(t *testing.T) {
	coll := setup(t)
	defer teardown(t, coll)

	locker := lock.NewMongoLocker(coll)

	// Create some locks.
	err := locker.XLock("resource1", "aaaa", lock.LockDetails{})
	if err != nil {
		t.Error(err)
	}
	err = locker.XLock("resource2", "aaaa", lock.LockDetails{})
	if err != nil {
		t.Error(err)
	}
	err = locker.XLock("resource3", "bbbb", lock.LockDetails{})
	if err != nil {
		t.Error(err)
	}

	// Try to lock something that's already locked.
	err = locker.XLock("resource1", "aaaa", lock.LockDetails{})
	if err != lock.ErrAlreadyLocked {
		t.Errorf("err = %s, expected the lock to fail due to the resource already being locked", err)
	}
	err = locker.XLock("resource1", "zzzz", lock.LockDetails{})
	if err != lock.ErrAlreadyLocked {
		t.Errorf("err = %s, expected the lock to fail due to the resource already being locked", err)
	}
}

func TestLockShared(t *testing.T) {
	coll := setup(t)
	defer teardown(t, coll)

	locker := lock.NewMongoLocker(coll)

	// Create some locks.
	err := locker.SLock("resource1", "aaaa", lock.LockDetails{}, 10)
	if err != nil {
		t.Error(err)
	}
	err = locker.SLock("resource1", "bbbb", lock.LockDetails{}, 10)
	if err != nil {
		t.Error(err)
	}
	err = locker.SLock("resource2", "bbbb", lock.LockDetails{}, 10)
	if err != nil {
		t.Error(err)
	}

	// Try to create a shared lock that already exists.
	err = locker.SLock("resource1", "aaaa", lock.LockDetails{}, 10)
	if err != lock.ErrAlreadyLocked {
		t.Errorf("err = %s, expected the lock to fail due to the resource already being locked", err)
	}
	err = locker.SLock("resource2", "bbbb", lock.LockDetails{}, 10)
	if err != lock.ErrAlreadyLocked {
		t.Errorf("err = %s, expected the lock to fail due to the resource already being locked", err)
	}
}

func TestLockMaxConcurrent(t *testing.T) {
	coll := setup(t)
	defer teardown(t, coll)

	locker := lock.NewMongoLocker(coll)

	// Create some locks.
	err := locker.SLock("resource1", "aaaa", lock.LockDetails{}, 2)
	if err != nil {
		t.Error(err)
	}
	err = locker.SLock("resource1", "bbbb", lock.LockDetails{}, 2)
	if err != nil {
		t.Error(err)
	}

	// Try to create a third lock, which will be more than maxConcurrent.
	err = locker.SLock("resource1", "cccc", lock.LockDetails{}, 2)
	if err != lock.ErrAlreadyLocked {
		t.Errorf("err = %s, expected the lock to fail due to the resource already being locked", err)
	}
}

func TestLockInteractions(t *testing.T) {
	coll := setup(t)
	defer teardown(t, coll)

	locker := lock.NewMongoLocker(coll)

	// Trying to create a shared lock on a resource that already has an
	// exclusive lock in it should return an error.
	err := locker.XLock("resource1", "aaaa", lock.LockDetails{})
	if err != nil {
		t.Error(err)
	}
	err = locker.SLock("resource1", "bbbb", lock.LockDetails{}, -1)
	if err != lock.ErrAlreadyLocked {
		t.Errorf("err = %s, expected the lock to fail due to the resource already being locked", err)
	}

	// Trying to create an exclusive lock on a resource that already has a
	// shared lock in it should return an error.
	err = locker.SLock("resource2", "aaaa", lock.LockDetails{}, -1)
	if err != nil {
		t.Error(err)
	}
	err = locker.XLock("resource2", "bbbb", lock.LockDetails{})
	if err != lock.ErrAlreadyLocked {
		t.Errorf("err = %s, expected the lock to fail due to the resource already being locked", err)
	}
}

func TestUnlock(t *testing.T) {
	coll := setup(t)
	defer teardown(t, coll)

	locker := lock.NewMongoLocker(coll)

	// Unlock an exclusive lock.
	err := locker.XLock("resource1", "aaaa", lock.LockDetails{})
	if err != nil {
		t.Error(err)
	}
	unlocked, err := locker.Unlock("aaaa")
	if err != nil {
		t.Error(err)
	}
	if len(unlocked) != 1 {
		t.Errorf("%d resources unlocked, expected %d", len(unlocked), 1)
	}
	if unlocked[0].Resource != "resource1" && unlocked[0].LockId != "aaaa" {
		t.Errorf("did not unlock the correct thing")
	}

	// Unlock a shared lock.
	err = locker.SLock("resource2", "bbbb", lock.LockDetails{}, -1)
	if err != nil {
		t.Error(err)
	}
	unlocked, err = locker.Unlock("bbbb")
	if err != nil {
		t.Error(err)
	}
	if len(unlocked) != 1 {
		t.Errorf("%d resources unlocked, expected %d", len(unlocked), 1)
	}
	if unlocked[0].Resource != "resource2" && unlocked[0].LockId != "bbbb" {
		t.Errorf("did not unlock the correct thing")
	}

	// Try to unlock a lockId that doesn't exist.
	unlocked, err = locker.Unlock("zzzz")
	if err != nil {
		t.Error(err)
	}
	if len(unlocked) != 0 {
		t.Errorf("%d resources unlocked, expected %d", len(unlocked), 0)
	}
}

func TestUnlockOrder(t *testing.T) {
	coll := setup(t)
	defer teardown(t, coll)

	locker := lock.NewMongoLocker(coll)

	// Create some locks.
	err := locker.XLock("resource1", "aaaa", lock.LockDetails{})
	if err != nil {
		t.Error(err)
	}
	err = locker.SLock("resource4", "aaaa", lock.LockDetails{}, -1)
	if err != nil {
		t.Error(err)
	}
	err = locker.XLock("resource3", "bbbb", lock.LockDetails{})
	if err != nil {
		t.Error(err)
	}
	err = locker.SLock("resource2", "bbbb", lock.LockDetails{}, -1)
	if err != nil {
		t.Error(err)
	}
	err = locker.SLock("resource2", "aaaa", lock.LockDetails{}, -1)
	if err != nil {
		t.Error(err)
	}

	// Make sure they are unlocked in the order of newest to oldest.
	unlocked, err := locker.Unlock("aaaa")
	if err != nil {
		t.Error(err)
	}

	actual := []string{}
	for _, lock := range unlocked {
		actual = append(actual, lock.Resource)
	}

	expected := []string{"resource2", "resource4", "resource1"}
	if diff := deep.Equal(actual, expected); diff != nil {
		t.Error(diff)
	}
}

func TestStatusFilters(t *testing.T) {
	coll := setup(t)
	defer teardown(t, coll)

	locker := lock.NewMongoLocker(coll)

	// Create a bunch of different locks. Sleep for a short time between
	// creating each lock to ensure none of them get the same CreatedAt
	// value. This is important because we will sort locks by their
	// CreatedAt value to make the tests below deterministic.
	aaaaDetails := lock.LockDetails{
		Owner: "john",
		Host:  "host.name",
		TTL:   3600,
	}
	bbbbDetails := lock.LockDetails{
		Owner: "smith",
	}
	ccccDetails := lock.LockDetails{
		TTL: 7200,
	}
	err := locker.XLock("resource1", "aaaa", aaaaDetails)
	if err != nil {
		t.Error(err)
	}
	time.Sleep(time.Duration(1) * time.Millisecond)
	err = locker.SLock("resource2", "aaaa", aaaaDetails, -1)
	if err != nil {
		t.Error(err)
	}
	time.Sleep(time.Duration(1) * time.Millisecond)
	err = locker.SLock("resource2", "bbbb", bbbbDetails, -1)
	if err != nil {
		t.Error(err)
	}
	time.Sleep(time.Duration(1) * time.Millisecond)
	recordedTime := time.Now() // Take a snapshot of the time...used later.
	time.Sleep(time.Duration(1) * time.Millisecond)
	err = locker.SLock("resource2", "cccc", ccccDetails, -1)
	if err != nil {
		t.Error(err)
	}
	time.Sleep(time.Duration(1) * time.Millisecond)
	err = locker.XLock("resource3", "bbbb", bbbbDetails)
	if err != nil {
		t.Error(err)
	}
	time.Sleep(time.Duration(1) * time.Millisecond)
	err = locker.SLock("resource4", "cccc", ccccDetails, -1)
	if err != nil {
		t.Error(err)
	}

	///////////////////////////////////////////////////////////////////////
	// Filter on TTL greater than.
	///////////////////////////////////////////////////////////////////////
	f := lock.Filter{
		TTLgt: 3700,
	}
	actual, err := locker.Status(f)
	if err != nil {
		t.Error(err)
	}

	// These must be in the order of LockStatusesByCreatedAtDesc.
	expected := []lock.LockStatus{
		lock.LockStatus{
			Resource: "resource4",
			LockId:   "cccc",
			Type:     lock.LOCK_TYPE_SHARED,
		},
		lock.LockStatus{
			Resource: "resource2",
			LockId:   "cccc",
			Type:     lock.LOCK_TYPE_SHARED,
		},
	}

	err = validateLockStatuses(actual, expected)
	if err != nil {
		t.Error(err)
	}

	///////////////////////////////////////////////////////////////////////
	// Filter on TTL less than. Shouldn't include locks with no TTL.
	///////////////////////////////////////////////////////////////////////
	f = lock.Filter{
		TTLlt: 600,
	}
	actual, err = locker.Status(f)
	if err != nil {
		t.Error(err)
	}

	expected = []lock.LockStatus{}

	err = validateLockStatuses(actual, expected)
	if err != nil {
		t.Error(err)
	}

	///////////////////////////////////////////////////////////////////////
	// Filter on CreatedAfter.
	///////////////////////////////////////////////////////////////////////
	f = lock.Filter{
		CreatedAfter: recordedTime,
	}
	actual, err = locker.Status(f)
	if err != nil {
		t.Error(err)
	}

	// These must be in the order of LockStatusesByCreatedAtDesc.
	expected = []lock.LockStatus{
		lock.LockStatus{
			Resource: "resource4",
			LockId:   "cccc",
			Type:     lock.LOCK_TYPE_SHARED,
		},
		lock.LockStatus{
			Resource: "resource3",
			LockId:   "bbbb",
			Type:     lock.LOCK_TYPE_EXCLUSIVE,
			Owner:    "smith",
		},
		lock.LockStatus{
			Resource: "resource2",
			LockId:   "cccc",
			Type:     lock.LOCK_TYPE_SHARED,
		},
	}

	err = validateLockStatuses(actual, expected)
	if err != nil {
		t.Error(err)
	}

	///////////////////////////////////////////////////////////////////////
	// Filter on CreatedBefore.
	///////////////////////////////////////////////////////////////////////
	f = lock.Filter{
		CreatedBefore: recordedTime,
	}
	actual, err = locker.Status(f)
	if err != nil {
		t.Error(err)
	}

	// These must be in the order of LockStatusesByCreatedAtDesc.
	expected = []lock.LockStatus{
		lock.LockStatus{
			Resource: "resource2",
			LockId:   "bbbb",
			Type:     lock.LOCK_TYPE_SHARED,
			Owner:    "smith",
		},
		lock.LockStatus{
			Resource: "resource2",
			LockId:   "aaaa",
			Type:     lock.LOCK_TYPE_SHARED,
			Owner:    "john",
			Host:     "host.name",
		},
		lock.LockStatus{
			Resource: "resource1",
			LockId:   "aaaa",
			Type:     lock.LOCK_TYPE_EXCLUSIVE,
			Owner:    "john",
			Host:     "host.name",
		},
	}

	err = validateLockStatuses(actual, expected)
	if err != nil {
		t.Error(err)
	}

	///////////////////////////////////////////////////////////////////////
	// Filter on Owner.
	///////////////////////////////////////////////////////////////////////
	f = lock.Filter{
		Owner: "smith",
	}
	actual, err = locker.Status(f)
	if err != nil {
		t.Error(err)
	}

	// These must be in the order of LockStatusesByCreatedAtDesc.
	expected = []lock.LockStatus{
		lock.LockStatus{
			Resource: "resource2",
			LockId:   "bbbb",
			Type:     lock.LOCK_TYPE_SHARED,
			Owner:    "smith",
		},
		lock.LockStatus{
			Resource: "resource3",
			LockId:   "bbbb",
			Type:     lock.LOCK_TYPE_EXCLUSIVE,
			Owner:    "smith",
		},
	}

	///////////////////////////////////////////////////////////////////////
	// Filter on TTL, Resource, and LockId.
	///////////////////////////////////////////////////////////////////////
	f = lock.Filter{
		TTLlt:    5000,
		Resource: "resource1",
		LockId:   "aaaa",
	}
	actual, err = locker.Status(f)
	if err != nil {
		t.Error(err)
	}

	// These must be in the order of LockStatusesByCreatedAtDesc.
	expected = []lock.LockStatus{
		lock.LockStatus{
			Resource: "resource1",
			LockId:   "aaaa",
			Type:     lock.LOCK_TYPE_EXCLUSIVE,
			Owner:    "john",
			Host:     "host.name",
		},
	}

	err = validateLockStatuses(actual, expected)
	if err != nil {
		t.Error(err)
	}

}

func TestStatusTTL(t *testing.T) {
	coll := setup(t)
	defer teardown(t, coll)

	locker := lock.NewMongoLocker(coll)

	// Create a lock with a TTL.
	err := locker.XLock("resource1", "aaaa", lock.LockDetails{TTL: 3600})
	if err != nil {
		t.Error(err)
	}

	// Make sure we get back a similar TTL when querying the status of the lock.
	f := lock.Filter{
		LockId: "aaaa",
	}
	actual, err := locker.Status(f)
	if err != nil {
		t.Error(err)
	}

	if len(actual) != 1 {
		t.Errorf("got the status of %d locks, expected %d", len(actual), 1)
	}

	if actual[0].TTL > 3600 || actual[0].TTL < 3575 {
		t.Errorf("ttl = %d, expected it to be between 3575 and 3600", actual[0].TTL)
	}
}

func TestRenew(t *testing.T) {
	coll := setup(t)
	defer teardown(t, coll)

	locker := lock.NewMongoLocker(coll)

	// Create some locks.
	err := locker.XLock("resource1", "aaaa", lock.LockDetails{TTL: 3600})
	if err != nil {
		t.Error(err)
	}
	err = locker.SLock("resource4", "aaaa", lock.LockDetails{TTL: 3600}, -1)
	if err != nil {
		t.Error(err)
	}
	err = locker.XLock("resource3", "bbbb", lock.LockDetails{})
	if err != nil {
		t.Error(err)
	}
	err = locker.SLock("resource2", "bbbb", lock.LockDetails{}, -1)
	if err != nil {
		t.Error(err)
	}
	err = locker.SLock("resource2", "aaaa", lock.LockDetails{TTL: 3600}, -1)
	if err != nil {
		t.Error(err)
	}

	// Verify that locks with the given lockId have their TTL updated.
	renewed, err := locker.Renew("aaaa", 7200)
	if err != nil {
		t.Error(err)
	}

	if len(renewed) != 3 {
		t.Errorf("%d locks renewed, expected %d", len(renewed), 3)
	}

	f := lock.Filter{
		LockId: "aaaa",
	}
	actual, err := locker.Status(f)
	if err != nil {
		t.Error(err)
	}

	if len(actual) != 3 {
		t.Errorf("got the status of %d locks, expected %d", len(actual), 1)
	}

	for _, a := range actual {
		if a.TTL > 7200 || a.TTL < 7175 {
			t.Errorf("ttl = %d for resource=%s lockId=%s, expected it to be between 7175 and 7200",
				a.TTL, a.Resource, a.LockId)
		}
	}
}

func TestRenewLockIdNotFound(t *testing.T) {
	coll := setup(t)
	defer teardown(t, coll)

	locker := lock.NewMongoLocker(coll)

	renewed, err := locker.Renew("aaaa", 7200)
	if err != lock.ErrLockNotFound {
		t.Errorf("err = %s, expected the renew to fail due to the lockId not existing", err)
	}

	if len(renewed) != 0 {
		t.Errorf("%d locks renewed, expected %d", len(renewed), 0)
	}
}

// ------------------------------------------------------------------------- //

// validateLockStatuses compares two slices of LockStatuses, returning an error
// if they are not the same. It zeros out some of the fields on the structs in
// the "actual" argument to make comparisons easier (and still accurate for the
// most part).
func validateLockStatuses(actual, expected []lock.LockStatus) error {
	// Sort actual to make checks deterministic. expected should already
	// be in the LockStatusesByCreatedAtDesc order, but we still need to
	// convert it to the correct type.
	var actualSorted lock.LockStatusesByCreatedAtDesc
	var expectedSorted lock.LockStatusesByCreatedAtDesc
	actualSorted = actual
	expectedSorted = expected
	sort.Sort(actualSorted)

	// Zero out some of the fields in the actual LockStatuses that make
	// it hard to do comparisons and also aren't necessary for this function.
	for i, lock := range actualSorted {
		tmp := lock
		tmp.CreatedAt = time.Time{}
		tmp.RenewedAt = nil
		tmp.TTL = 0
		actualSorted[i] = tmp
	}

	if diff := deep.Equal(actualSorted, expectedSorted); diff != nil {
		return fmt.Errorf("%#v", diff)
	}

	return nil
}
