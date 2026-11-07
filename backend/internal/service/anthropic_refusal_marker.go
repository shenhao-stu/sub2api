package service

import (
	"crypto/sha256"
	"fmt"
	"os"
)

// Protected instances share this private directory, including during a rolling
// release. The marker contains only a digest, never session metadata or content.
func openAnthropicRefusalJournal(dir string) (*os.Root, error) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	info, err := root.Stat(".")
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 {
		root.Close()
		return nil, fmt.Errorf("invalid Anthropic refusal journal directory")
	}
	return root, nil
}

func anthropicRefusalMarkerName(key string) string {
	return fmt.Sprintf("%x.block", sha256.Sum256([]byte(key)))
}

func hasAnthropicRefusalMarker(root *os.Root, key string) (bool, error) {
	info, err := root.Lstat(anthropicRefusalMarkerName(key))
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || info.Size() != 0 {
		return false, fmt.Errorf("invalid Anthropic refusal marker")
	}
	return true, nil
}

func recordAnthropicRefusalMarker(dir, key string) error {
	root, err := openAnthropicRefusalJournal(dir)
	if err != nil {
		return err
	}
	defer root.Close()
	name := anthropicRefusalMarkerName(key)
	f, err := root.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if os.IsExist(err) {
		if exists, checkErr := hasAnthropicRefusalMarker(root, key); checkErr != nil || !exists {
			return fmt.Errorf("invalid existing Anthropic refusal marker")
		}
		f, err = root.OpenFile(name, os.O_RDONLY, 0)
	}
	if err != nil {
		return err
	}
	defer f.Close()
	if err := f.Sync(); err != nil {
		return err
	}
	directory, err := root.Open(".")
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func readAnthropicRefusalMarker(dir, key string) (bool, error) {
	root, err := openAnthropicRefusalJournal(dir)
	if err != nil {
		return false, err
	}
	defer root.Close()
	return hasAnthropicRefusalMarker(root, key)
}
