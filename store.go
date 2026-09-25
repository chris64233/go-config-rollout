package configrollout

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
)

// Store 持久化完整状态快照。状态数据与 outbox 同处一个快照，
// 因此 Save 的原子性即保证“业务状态 + outbox”一起提交或一起不提交。
type Store interface {
	// Load 在尚无快照时返回 (nil, nil)。
	Load(ctx context.Context) (*Data, error)
	Save(ctx context.Context, data *Data) error
}

// FileStore 以 JSON 文件保存快照：先写临时文件再 rename，
// 进程崩溃时磁盘上要么是旧快照、要么是新快照，不会出现半截文件。
type FileStore struct {
	path string
}

// NewFileStore 创建指向 path 的文件存储（目录会自动创建）。
func NewFileStore(path string) *FileStore {
	return &FileStore{path: path}
}

func (s *FileStore) Load(_ context.Context) (*Data, error) {
	raw, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var d Data
	if err := json.Unmarshal(raw, &d); err != nil {
		return nil, err
	}
	if d.Version == 0 {
		return nil, errors.New("stored snapshot has invalid version")
	}
	return &d, nil
}

func (s *FileStore) Save(ctx context.Context, data *Data) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".rollout-snapshot-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // rename 成功后此调用无害
	if _, err := tmp.Write(raw); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, s.path)
}

// MemoryStore 把快照保存在内存中，主要用于测试。
type MemoryStore struct {
	mu   sync.Mutex
	data *Data
}

// NewMemoryStore 创建空的内存存储。
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{}
}

func (s *MemoryStore) Load(_ context.Context) (*Data, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data == nil {
		return nil, nil
	}
	return cloneData(s.data), nil
}

func (s *MemoryStore) Save(_ context.Context, data *Data) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = cloneData(data)
	return nil
}
