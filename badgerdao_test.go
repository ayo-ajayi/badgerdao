package badgerdao

import (
	"testing"

	"github.com/dgraph-io/badger/v3"
)

func setupTestRepository(t *testing.T) *EntityRepository {
	dbPath := "" 
	repo, err := NewEntityRepository(badger.DefaultOptions(dbPath).WithInMemory(true).WithLogger(nil))
	if err != nil {
		t.Fatalf("Error creating entity repository: %v", err)
	}
	return repo
}

func TestEntityRepository_PutAndGet(t *testing.T) {
	repo := setupTestRepository(t)
	defer repo.Close()

	tests := []struct {
		key   []byte
		value []byte
	}{
		{[]byte("key1"), []byte("value1")},
		{[]byte("key2"), []byte("value2")},
		{[]byte("key3"), []byte("value3")},
	}

	for _, test := range tests {
		err := repo.Put(test.key, test.value)
		if err != nil {
			t.Fatalf("Error putting value for key %s: %v", string(test.key), err)
		}

		gotValue, err := repo.Get(test.key)
		if err != nil {
			t.Fatalf("Error getting value for key %s: %v", string(test.key), err)
		}

		if string(test.value) != string(gotValue) {
			t.Fatalf("Expected value %s for key %s, but got %s", string(test.value), string(test.key), string(gotValue))
		}
	}
}

func TestEntityRepository_KeyExists(t *testing.T) {
	repo := setupTestRepository(t)
	defer repo.Close()

	key1 := []byte("key1")
	nonExistentKey := []byte("non_existent_key")

	err := repo.Put(key1, []byte("value1"))
	if err != nil {
		t.Fatalf("Error putting value for key %s: %v", string(key1), err)
	}

	exists, err := repo.KeyExists(key1)
	if err != nil {
		t.Fatalf("Error checking if key %s exists: %v", string(key1), err)
	}
	if !exists {
		t.Fatalf("Expected key %s to exist, but it does not", string(key1))
	}

	exists, err = repo.KeyExists(nonExistentKey)
	if err != nil {
		t.Fatalf("Error checking if key %s exists: %v", string(nonExistentKey), err)
	}
	if exists {
		t.Fatalf("Expected key %s to not exist, but it does", string(nonExistentKey))
	}
}

func TestEntityRepository_Delete(t *testing.T) {
	repo := setupTestRepository(t)
	defer repo.Close()

	key := []byte("key2")

	err := repo.Put(key, []byte("value2"))
	if err != nil {
		t.Fatalf("Error putting value for key %s: %v", string(key), err)
	}

	err = repo.Delete(key)
	if err != nil {
		t.Fatalf("Error deleting value for key %s: %v", string(key), err)
	}

	exists, err := repo.KeyExists(key)
	if err != nil {
		t.Fatalf("Error checking if key %s exists: %v", string(key), err)
	}
	if exists {
		t.Fatalf("Expected key %s to be deleted, but it still exists", string(key))
	}
}

func TestEntityRepository_GetAll(t *testing.T) {
	repo := setupTestRepository(t)
	defer repo.Close()

	tests := []struct {
		Key   []byte
		Value []byte
	}{
		{[]byte("key1"), []byte("value1")},
		{[]byte("key3"), []byte("value3")},
	}

	for _, test := range tests {
		err := repo.Put(test.Key, test.Value)
		if err != nil {
			t.Fatalf("Error putting value for key %s: %v", string(test.Key), err)
		}
	}

	entities, err := repo.GetAll()
	if err != nil {
		t.Fatalf("Error getting all entities: %v", err)
	}

	if len(entities) != len(tests) {
		t.Fatalf("Expected %d entities, but got %d", len(tests), len(entities))
	}

	for _, test := range tests {
		found := false
		for _, entity := range entities {
			if string(test.Key) == string(entity.Key) && string(test.Value) == string(entity.Value) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("Expected entity with key %s and value %s, but not found in GetAll result", string(test.Key), string(test.Value))
		}
	}
}
