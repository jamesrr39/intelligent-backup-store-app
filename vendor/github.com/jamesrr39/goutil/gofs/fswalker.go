package gofs

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/jamesrr39/goutil/patternmatcher"
	"github.com/jamesrr39/semaphore"
)

type WalkOptions struct {
	IncludesMatcher,
	ExcludesMatcher patternmatcher.Matcher
	MaxConcurrency uint
}

type walkerType struct {
	fs              Fs
	basePath        string
	walkFunc        filepath.WalkFunc
	options         WalkOptions
	processSema     *semaphore.Semaphore
	errChan         chan error
	addToQueueWg    *sync.WaitGroup
	processPathChan chan string
}

const DefaultMaxConcurrency = 1

// Walk walks a tree concurrently
// If options.MaxConcurrency is set to >1, any actions you do in the provided walkFunc must be synchronized (or not need to be synchronized)
func Walk(fs Fs, path string, walkFunc filepath.WalkFunc, options WalkOptions) error {
	maxConcurrency := options.MaxConcurrency
	if maxConcurrency == 0 {
		maxConcurrency = DefaultMaxConcurrency
	}

	wt := &walkerType{
		fs:              fs,
		basePath:        path,
		walkFunc:        walkFunc,
		options:         options,
		processSema:     semaphore.NewSemaphore(maxConcurrency),
		errChan:         make(chan error),
		addToQueueWg:    new(sync.WaitGroup),
		processPathChan: make(chan string, maxConcurrency),
	}

	doneChan := make(chan error)

	go func() {
		for {
			select {
			case err := <-wt.errChan:
				doneChan <- err
				return

			case path := <-wt.processPathChan:
				slog.Debug("picking up new path", "path", path)
				wt.processSema.Add()
				slog.Debug("picked up new path", "path", path)
				func(path string) {
					defer wt.addToQueueWg.Done()
					defer func() {
						slog.Debug("fswalker: about to call Done", "path", path)
						wt.processSema.Done()
						slog.Debug("fswalker: Done called. Now: %d\n", path, wt.processSema.CurrentlyRunning())
					}()
					fileInfo, err := wt.processPath(path)
					if err != nil {
						wt.errChan <- err
						return
					}

					if fileInfo == nil {
						// path was excluded, nothing more to do
						return
					}

					if fileInfo.IsDir() {
						err = wt.walkDir(path)
						if err != nil {
							wt.errChan <- err
							return
						}
					}
				}(path)
			}
		}
	}()

	wt.addToQueueWg.Add(1)
	wt.processPathChan <- path

	go func() {
		wt.addToQueueWg.Wait()
		doneChan <- nil
	}()

	err := <-doneChan

	return err
}

func (wt *walkerType) processPath(path string) (os.FileInfo, error) {
	relativePath := strings.TrimPrefix(strings.TrimPrefix(path, wt.basePath), string(filepath.Separator))
	isExcluded := wt.options.ExcludesMatcher != nil && wt.options.ExcludesMatcher.Matches(relativePath)
	if isExcluded {
		return nil, nil
	}

	if wt.options.IncludesMatcher != nil {
		isIncluded := wt.options.IncludesMatcher.Matches(relativePath)
		if !isIncluded {
			return nil, nil
		}
	}

	fileInfo, err := wt.fs.Lstat(path)
	if err != nil {
		return nil, err
	}

	err = wt.walkFunc(path, fileInfo, nil)
	if err != nil {
		return nil, err
	}

	return fileInfo, nil
}

func (wt *walkerType) walkDir(path string) error {
	dirEntryInfos, err := wt.fs.ReadDir(path)
	if err != nil {
		return err
	}

	slog.Debug("about to put in dirEntryInfos", "items in directory", len(dirEntryInfos), "items in channel count", len(wt.processPathChan), "path", path)

	wt.addToQueueWg.Add(len(dirEntryInfos))
	for _, dirEntryInfo := range dirEntryInfos {
		childPath := filepath.Join(path, dirEntryInfo.Name())

		go func(childPath string) {
			wt.processPathChan <- childPath
		}(childPath)
	}

	return nil
}
