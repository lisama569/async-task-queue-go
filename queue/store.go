package queue

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"sync"
)

// store 是基于本地 JSON 文件的持久化存储。
// 所有变更在持锁状态下先落盘再返回，保证崩溃后可从磁盘恢复。
type store struct {
	mu    sync.Mutex
	path  string
	tasks map[string]*Task
}

func openStore(path string) (*store, error) {
	s := &store{path: path, tasks: map[string]*Task{}}
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return s, nil
	}
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return s, nil
	}
	var tasks map[string]*Task
	if err := json.Unmarshal(data, &tasks); err != nil {
		return nil, err
	}
	s.tasks = tasks
	return s, nil
}

// persistLocked 将全量任务原子写盘（临时文件 + rename）。调用前必须持锁。
func (s *store) persistLocked() error {
	data, err := json.MarshalIndent(s.tasks, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

func (s *store) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.persistLocked()
}
