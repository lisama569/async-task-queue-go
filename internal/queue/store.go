package queue

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// Store 是任务的本地持久化存储（单个 JSON 文件，写时替换，保证原子性）。
type Store struct {
	path string
}

// NewStore 打开（必要时创建）位于 path 的本地存储。
func NewStore(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("queue: create store dir: %w", err)
	}
	return &Store{path: path}, nil
}

// load 读取全部任务；文件不存在时返回空集。
func (s *Store) load() (map[string]*Task, error) {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, fs.ErrNotExist) {
		return map[string]*Task{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("queue: read store: %w", err)
	}
	tasks := map[string]*Task{}
	if len(data) == 0 {
		return tasks, nil
	}
	if err := json.Unmarshal(data, &tasks); err != nil {
		return nil, fmt.Errorf("queue: decode store: %w", err)
	}
	return tasks, nil
}

// save 以“写临时文件 + 重命名”的方式原子持久化全部任务。
func (s *Store) save(tasks map[string]*Task) error {
	data, err := json.MarshalIndent(tasks, "", "  ")
	if err != nil {
		return fmt.Errorf("queue: encode store: %w", err)
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("queue: write store: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("queue: commit store: %w", err)
	}
	return nil
}
