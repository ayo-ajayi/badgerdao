package badgerdao

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v3"
)

type Entity struct {
	Key   []byte
	Value []byte
}

type EntityRepository struct {
	db *badger.DB
}

func NewEntityRepository(opts badger.Options) (*EntityRepository, error) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovers from panic", r)
		}
	}()
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &EntityRepository{
		db: db,
	}, nil
}

func (e *EntityRepository) Close() error {
	return e.db.Close()
}

func (e *EntityRepository) Put(key, value []byte) error {
	return e.db.Update(func(txn *badger.Txn) error {
		entry := badger.NewEntry(key, value).WithTTL(5 * time.Second)
		return txn.SetEntry(entry)
	})
}

func (e *EntityRepository) KeyExists(key []byte) (bool, error) {
	err := e.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		return err
	})
	if err == nil {
		return true, nil
	} else if err == badger.ErrKeyNotFound {
		return false, nil
	} else {
		return false, err
	}
}

func (e *EntityRepository) Get(key []byte) ([]byte, error) {
	var value []byte
	err := e.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		value, err = item.ValueCopy(nil)
		return err
	})
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (e *EntityRepository) GetAll() ([]Entity, error) {
	var entities []Entity
	err := e.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		iter := txn.NewIterator(opts)
		defer iter.Close()
		for iter.Rewind(); iter.Valid(); iter.Next() {
			item := iter.Item()
			key := item.KeyCopy(nil)
			var value []byte
			err := item.Value(func(v []byte) error {
				value = append([]byte{}, v...)
				return nil
			})
			if err != nil {
				return err
			}
			entities = append(entities, Entity{Key: key, Value: value})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return entities, nil
}

func (e *EntityRepository) Delete(key []byte) error {
	return e.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})
}

func (e *EntityRepository) GenerateUniqueID() ([]byte, error) {
	timestampNs := time.Now().UnixNano()
	identifier := "000"
	for {
		id := fmt.Sprintf("%d-%s", timestampNs, identifier)
		checkKeyExists, err := e.KeyExists([]byte(id))
		if err != nil {
			return nil, fmt.Errorf("failed to check if key exists: %v", err)
		}
		if !checkKeyExists {
			return []byte(id), nil
		}
		identifierInt, err := strconv.Atoi(identifier)
		if err != nil {
			return nil, fmt.Errorf("failed to parse identifier: %v", err)
		}
		identifierInt++
		identifier = fmt.Sprintf("%03d", identifierInt)
		if identifierInt > 999 {
			return nil, fmt.Errorf("maximum number of iterations reached")
		}
	}
}

func (e *EntityRepository) GetDateFromUniqueID(id string) (string, error) {
	parts := strings.Split(id, "-")
	if len(parts) != 2 {
		return "", fmt.Errorf("invalid unique ID format")
	}
	timestampStr := parts[0]
	timestampMs, err := strconv.ParseInt(timestampStr, 10, 64)
	if err != nil {
		return "", fmt.Errorf("failed to parse timestamp from ID: %v", err)
	}
	timestampSec := timestampMs / 1e9
	t := time.Unix(timestampSec, 0).UTC()
	return t.Format("2006-01-02 15:04:05"), nil
}
