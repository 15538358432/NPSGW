package logfile

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

const name = "nps.log"

// Writer 将日志追加到 nps.log，并按大小轮转。编号越大，归档越旧。
type Writer struct {
	mu       sync.Mutex
	path     string
	maxBytes int64
	maxFiles int
	file     *os.File
	size     int64
}

// Open 创建日志目录并打开当前日志。maxFiles 包含当前日志。
func Open(dir string, maxBytes int64, maxFiles int) (*Writer, error) {
	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, err
	}
	writer := &Writer{
		path:     filepath.Join(dir, name),
		maxBytes: maxBytes,
		maxFiles: maxFiles,
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		suffix, ok := strings.CutPrefix(entry.Name(), name+".")
		if !ok || !entry.Type().IsRegular() {
			continue
		}
		index, err := strconv.Atoi(suffix)
		if err == nil && index >= maxFiles && strconv.Itoa(index) == suffix {
			if err := removeIfExists(filepath.Join(dir, entry.Name())); err != nil {
				return nil, err
			}
		}
	}
	if err := writer.openCurrent(); err != nil {
		return nil, err
	}
	return writer, nil
}

func (w *Writer) openCurrent() error {
	file, err := os.OpenFile(w.path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0640)
	if err != nil {
		return err
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return err
	}
	w.file = file
	w.size = info.Size()
	return nil
}

// Write 尽量将一条日志完整写在同一文件。超过单文件上限的日志分段写入。
func (w *Writer) Write(p []byte) (written int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	defer func() {
		if err != nil {
			fmt.Fprintf(os.Stderr, "[ERROR] 写入日志 %s 失败: %v\n", w.path, err)
			os.Stderr.Write(p)
		}
	}()

	for len(p) > 0 {
		if w.file == nil {
			if err := w.openCurrent(); err != nil {
				return written, err
			}
		}
		remaining := w.maxBytes - w.size
		if remaining <= 0 || (w.size > 0 && written == 0 && int64(len(p)) > remaining) {
			if err := w.rotate(); err != nil {
				return written, err
			}
			continue
		}
		chunk := min(int64(len(p)), remaining)
		n, err := w.file.Write(p[:chunk])
		w.size += int64(n)
		written += n
		p = p[n:]
		if err != nil {
			return written, err
		}
	}
	return written, nil
}

func (w *Writer) rotate() error {
	if err := w.closeCurrent(); err != nil {
		return err
	}
	if w.maxFiles == 1 {
		return os.Remove(w.path)
	}
	backup := func(index int) string {
		return w.path + "." + strconv.Itoa(index)
	}
	if err := removeIfExists(backup(w.maxFiles - 1)); err != nil {
		return err
	}
	for index := w.maxFiles - 2; index >= 1; index-- {
		if err := os.Rename(backup(index), backup(index+1)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return os.Rename(w.path, backup(1))
}

func removeIfExists(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (w *Writer) closeCurrent() error {
	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	return err
}

func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.closeCurrent()
}
