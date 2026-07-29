package resources

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	bolt "go.etcd.io/bbolt"
)

const (
	durableRegistryMode    = "bbolt_v1"
	durableSchemaVersion   = uint32(1)
	durableOpenLockTimeout = 250 * time.Millisecond
)

var (
	metadataBucket    = []byte("meta")
	resourcesBucket   = []byte("resources")
	generationsBucket = []byte("generations")
	tokensBucket      = []byte("tokens")
	schemaVersionKey  = []byte("schema_version")
)

type durableStore struct {
	database *bolt.DB
}

// OpenDurableRegistry opens the single-owner transactional metadata registry at
// path. Corrupt or unknown schemas fail closed rather than starting empty.
func OpenDurableRegistry(path string) (*Registry, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("control metadata path is required")
	}
	path = filepath.Clean(path)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create control metadata directory: %w", err)
	}
	database, err := bolt.Open(path, 0o600, &bolt.Options{
		Timeout: durableOpenLockTimeout,
		NoSync:  false,
	})
	if err != nil {
		return nil, fmt.Errorf("open control metadata database: %w", err)
	}
	store := &durableStore{database: database}
	closeWith := func(openError error) (*Registry, error) {
		return nil, errors.Join(openError, database.Close())
	}
	if err := os.Chmod(path, 0o600); err != nil {
		return closeWith(fmt.Errorf("secure control metadata database: %w", err))
	}
	if err := database.Update(initializeDurableSchema); err != nil {
		return closeWith(fmt.Errorf("initialize control metadata database: %w", err))
	}
	state, err := store.load()
	if err != nil {
		return closeWith(fmt.Errorf("load control metadata database: %w", err))
	}
	return newRegistry(state, store), nil
}

func initializeDurableSchema(transaction *bolt.Tx) error {
	metadata := transaction.Bucket(metadataBucket)
	if metadata == nil {
		var err error
		if metadata, err = transaction.CreateBucket(metadataBucket); err != nil {
			return err
		}
		for _, name := range [][]byte{resourcesBucket, generationsBucket, tokensBucket} {
			if _, err := transaction.CreateBucket(name); err != nil {
				return err
			}
		}
		return metadata.Put(schemaVersionKey, encodeSchemaVersion(durableSchemaVersion))
	}

	version := metadata.Get(schemaVersionKey)
	if len(version) != 4 {
		return fmt.Errorf("missing or malformed schema version")
	}
	if actual := binary.BigEndian.Uint32(version); actual != durableSchemaVersion {
		return fmt.Errorf(
			"unsupported schema version %d, expected %d",
			actual,
			durableSchemaVersion,
		)
	}
	for _, name := range [][]byte{resourcesBucket, generationsBucket, tokensBucket} {
		if transaction.Bucket(name) == nil {
			return fmt.Errorf("required bucket %q is missing", name)
		}
	}
	return nil
}

func encodeSchemaVersion(version uint32) []byte {
	encoded := make([]byte, 4)
	binary.BigEndian.PutUint32(encoded, version)
	return encoded
}

func (store *durableStore) Mode() string {
	return durableRegistryMode
}

func (store *durableStore) Close() error {
	return store.database.Close()
}

func (store *durableStore) Commit(mutation registryMutation) error {
	if mutation.resource != nil && mutation.deleteResource {
		return fmt.Errorf("resource mutation cannot write and delete simultaneously")
	}
	if mutation.generation != nil && *mutation.generation == 0 {
		return fmt.Errorf("resource generation mutation must be positive")
	}

	var (
		key              []byte
		resourceDocument []byte
		tokenDocument    []byte
		err              error
	)
	if mutation.resource != nil || mutation.deleteResource || mutation.generation != nil {
		key, err = encodeResourceKey(mutation.resourceKey)
		if err != nil {
			return err
		}
	}
	if mutation.resource != nil {
		resource := cloneResource(*mutation.resource)
		if resource.ResourceKey != mutation.resourceKey {
			return fmt.Errorf("resource mutation key does not match its value")
		}
		resourceDocument, err = json.Marshal(resource)
		if err != nil {
			return fmt.Errorf("encode resource mutation: %w", err)
		}
	}
	if mutation.tokenRecord != nil {
		if err := validateStoredToken(mutation.token, *mutation.tokenRecord); err != nil {
			return err
		}
		record := cloneTokenRecord(*mutation.tokenRecord)
		tokenDocument, err = json.Marshal(record)
		if err != nil {
			return fmt.Errorf("encode request token mutation: %w", err)
		}
	} else if mutation.token != "" {
		return fmt.Errorf("request token mutation is missing its value")
	}

	return store.database.Update(func(transaction *bolt.Tx) error {
		resources := transaction.Bucket(resourcesBucket)
		generations := transaction.Bucket(generationsBucket)
		tokens := transaction.Bucket(tokensBucket)
		if resources == nil || generations == nil || tokens == nil {
			return fmt.Errorf("durable registry schema is incomplete")
		}
		if mutation.deleteResource {
			if err := resources.Delete(key); err != nil {
				return err
			}
		} else if mutation.resource != nil {
			if err := resources.Put(key, resourceDocument); err != nil {
				return err
			}
		}
		if mutation.generation != nil {
			encoded := make([]byte, 8)
			binary.BigEndian.PutUint64(encoded, *mutation.generation)
			if err := generations.Put(key, encoded); err != nil {
				return err
			}
		}
		if mutation.tokenRecord != nil {
			if err := tokens.Put([]byte(mutation.token), tokenDocument); err != nil {
				return err
			}
		}
		return nil
	})
}

func (store *durableStore) load() (registryState, error) {
	state := emptyRegistryState()
	err := store.database.View(func(transaction *bolt.Tx) error {
		generations := transaction.Bucket(generationsBucket)
		resources := transaction.Bucket(resourcesBucket)
		tokens := transaction.Bucket(tokensBucket)
		if generations == nil || resources == nil || tokens == nil {
			return fmt.Errorf("durable registry schema is incomplete")
		}
		if err := generations.ForEach(func(rawKey, rawValue []byte) error {
			if rawValue == nil {
				return fmt.Errorf("generation key contains a nested bucket")
			}
			key, err := decodeResourceKey(rawKey)
			if err != nil {
				return fmt.Errorf("decode generation key: %w", err)
			}
			if _, duplicate := state.lastGeneration[key]; duplicate {
				return fmt.Errorf("duplicate canonical generation key")
			}
			if len(rawValue) != 8 {
				return fmt.Errorf("generation for %q is malformed", rawKey)
			}
			generation := binary.BigEndian.Uint64(rawValue)
			if generation == 0 {
				return fmt.Errorf("generation for %q must be positive", rawKey)
			}
			state.lastGeneration[key] = generation
			return nil
		}); err != nil {
			return err
		}
		if err := resources.ForEach(func(rawKey, rawValue []byte) error {
			if rawValue == nil {
				return fmt.Errorf("resource key contains a nested bucket")
			}
			key, err := decodeResourceKey(rawKey)
			if err != nil {
				return fmt.Errorf("decode resource key: %w", err)
			}
			if _, duplicate := state.resources[key]; duplicate {
				return fmt.Errorf("duplicate canonical resource key")
			}
			var resource Resource
			if err := decodeStoredJSON(rawValue, &resource); err != nil {
				return fmt.Errorf("decode resource %q: %w", rawKey, err)
			}
			if err := validateStoredResource(key, resource); err != nil {
				return fmt.Errorf("validate resource %q: %w", rawKey, err)
			}
			generation, found := state.lastGeneration[key]
			if !found || generation != resource.Generation {
				return fmt.Errorf("resource generation does not match its generation record")
			}
			state.resources[key] = cloneResource(resource)
			return nil
		}); err != nil {
			return err
		}
		return tokens.ForEach(func(rawToken, rawValue []byte) error {
			if rawValue == nil {
				return fmt.Errorf("request token contains a nested bucket")
			}
			token := string(rawToken)
			if _, duplicate := state.tokens[token]; duplicate {
				return fmt.Errorf("duplicate request token")
			}
			var record tokenRecord
			if err := decodeStoredJSON(rawValue, &record); err != nil {
				return fmt.Errorf("decode request token %q: %w", token, err)
			}
			if err := validateStoredToken(token, record); err != nil {
				return fmt.Errorf("validate request token %q: %w", token, err)
			}
			state.tokens[token] = cloneTokenRecord(record)
			return nil
		})
	})
	if err != nil {
		return registryState{}, err
	}
	return state, nil
}

func encodeResourceKey(key ResourceKey) ([]byte, error) {
	normalized, err := normalizeKey(key)
	if err != nil {
		return nil, err
	}
	if normalized != key {
		return nil, fmt.Errorf("resource key is not canonical")
	}
	encoded, err := json.Marshal(key)
	if err != nil {
		return nil, fmt.Errorf("encode resource key: %w", err)
	}
	return encoded, nil
}

func decodeResourceKey(encoded []byte) (ResourceKey, error) {
	var key ResourceKey
	if err := decodeStoredJSON(encoded, &key); err != nil {
		return ResourceKey{}, err
	}
	normalized, err := normalizeKey(key)
	if err != nil {
		return ResourceKey{}, err
	}
	if normalized != key {
		return ResourceKey{}, fmt.Errorf("resource key is not canonical")
	}
	canonical, err := json.Marshal(key)
	if err != nil {
		return ResourceKey{}, err
	}
	if !bytes.Equal(encoded, canonical) {
		return ResourceKey{}, fmt.Errorf("resource key encoding is not canonical")
	}
	return key, nil
}

func validateStoredResource(key ResourceKey, resource Resource) error {
	normalized, err := normalizeKey(key)
	if err != nil || normalized != key {
		return fmt.Errorf("resource identity is invalid")
	}
	if resource.ResourceKey != key {
		return fmt.Errorf("resource identity does not match its key")
	}
	if resource.Generation == 0 {
		return fmt.Errorf("resource generation must be positive")
	}
	canonicalSpec, err := canonicalJSON(resource.Spec)
	if err != nil || !bytes.Equal(canonicalSpec, resource.Spec) {
		return fmt.Errorf("resource spec is not canonical JSON")
	}
	if err := validateStatus(resource.Status, resource.Generation); err != nil {
		return err
	}
	return nil
}

func validateStoredToken(token string, record tokenRecord) error {
	if strings.TrimSpace(token) != token {
		return fmt.Errorf("request token is not canonical")
	}
	if err := validateToken(token); err != nil {
		return err
	}
	if len(record.Fingerprint) != sha256HexLength {
		return fmt.Errorf("request fingerprint is malformed")
	}
	if _, err := hex.DecodeString(record.Fingerprint); err != nil {
		return fmt.Errorf("request fingerprint is malformed")
	}
	switch record.Operation {
	case "apply":
		if record.Apply == nil || record.Delete != nil {
			return fmt.Errorf("apply token has the wrong result shape")
		}
		if record.Apply.Replayed {
			return fmt.Errorf("stored apply result cannot be marked replayed")
		}
		key := record.Apply.Resource.ResourceKey
		if err := validateStoredResource(key, record.Apply.Resource); err != nil {
			return fmt.Errorf("stored apply result: %w", err)
		}
	case "delete":
		if record.Delete == nil || record.Apply != nil {
			return fmt.Errorf("delete token has the wrong result shape")
		}
		if record.Delete.Replayed {
			return fmt.Errorf("stored delete result cannot be marked replayed")
		}
		normalized, err := normalizeKey(record.Delete.Key)
		if err != nil || normalized != record.Delete.Key {
			return fmt.Errorf("stored delete result has an invalid key")
		}
	default:
		return fmt.Errorf("unknown request token operation %q", record.Operation)
	}
	return nil
}

const sha256HexLength = 64

func decodeStoredJSON(encoded []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err == nil {
		return fmt.Errorf("multiple JSON values")
	} else if !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}
