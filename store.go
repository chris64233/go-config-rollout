package configrollout

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Store 是发布状态与 outbox 的持久化抽象。
//
// 所有状态变更都必须通过 mutate 在单个事务内完成：业务逻辑在状态快照上
// 修改并追加 outbox 事件，提交时整体替换并落盘。多个并发调用串行化执行，
// 因此“恢复 / 暂停 / 取消同时发生”时最终只会留下一条单调状态序列。
type Store interface {
	mutate(fn func(*State) error) error
	read(fn func(*State) error) error
	Close() error
}

// MemoryStore 把状态保存在内存中，可选用 JSON 文件做进程重启后的持久化。
type MemoryStore struct {
	mu    sync.Mutex
	state *State
	path  string // 非空时，每次提交后原子落盘
}

// NewMemoryStore 创建一个空的内存存储（不跨进程持久化）。
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{state: newState()}
}

// NewFileStore 打开（不存在则创建）一个以 JSON 文件持久化的存储。
func NewFileStore(path string) (*MemoryStore, error) {
	s := &MemoryStore{state: newState(), path: path}
	data, err := os.ReadFile(path)
	switch {
	case os.IsNotExist(err):
		// 首次使用，先落一个空快照。
		if err := s.persistLocked(); err != nil {
			return nil, err
		}
	case err != nil:
		return nil, err
	default:
		st := newState()
		if err := json.Unmarshal(data, st); err != nil {
			return nil, fmt.Errorf("corrupt rollout state file %q: %w", path, err)
		}
		s.state = st
	}
	return s, nil
}

func (s *MemoryStore) mutate(fn func(*State) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 在深拷贝上执行，业务返回错误时整笔事务丢弃，已提交状态不受影响。
	candidate := s.state.clone()
	if err := fn(candidate); err != nil {
		return err
	}
	s.state = candidate
	if s.path != "" {
		if err := s.persistLocked(); err != nil {
			// 理论上不会发生（调用方持锁）；落盘失败即提交失败。
			return err
		}
	}
	return nil
}

func (s *MemoryStore) read(fn func(*State) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return fn(s.state)
}

// Close 对内存实现无额外操作；文件存储的每次提交都已 fsync。
func (s *MemoryStore) Close() error { return nil }

// persistLocked 将快照原子写入文件：同目录临时文件 + fsync + rename。
// 调用方必须持有 mu。
func (s *MemoryStore) persistLocked() error {
	if s.path == "" {
		return nil
	}
	data, err := json.MarshalIndent(s.state, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(s.path)
	tmp, err := os.CreateTemp(dir, ".rollout-state-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpName)
		}
	}()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		return err
	}
	cleanup = false
	return nil
}
