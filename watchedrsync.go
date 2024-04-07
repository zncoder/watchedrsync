package main

import (
	"flag"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/zncoder/check"
	"github.com/zncoder/mygo"
)

var (
	baseDir            string
	remotePath         string
	shallow            bool
	guessText          bool
	eventDelayDuration time.Duration
	parallel           int
	verbose            bool
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	flag.StringVar(&remotePath, "r", "", "remote parent dir in rsync format, host:dir")
	flag.BoolVar(&shallow, "s", false, "watch just local_dir, not subdirs")
	flag.BoolVar(&guessText, "t", false, "guess file content is text")
	flag.IntVar(&parallel, "p", 10, "parallelism")
	flag.DurationVar(&eventDelayDuration, "d", 2*time.Second, "delay to batch processing events")
	flag.BoolVar(&verbose, "v", true, "verbose")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s local_dir\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()
	check.T(remotePath != "").F("no remote dir")
	check.T(flag.NArg() == 1).F("no local dir")

	baseDir = flag.Arg(0)
	baseDir = check.V(filepath.Abs(baseDir)).F("invalid dir", "dir", baseDir)
	check.T(mygo.IsDir(baseDir)).F("not a dir", "dir", baseDir)
	dir := filepath.Base(baseDir)
	baseDir += "/" // for rsync
	remotePath = filepath.Join(filepath.Clean(remotePath), dir) + "/"
	check.L("sync to remote", "dir", remotePath)

	watcher := check.V(fsnotify.NewBufferedWatcher(50)).F("NewWatcher")
	watchDir(watcher, baseDir, !shallow)

	start := time.Now()
	rsync(baseDir, remotePath)
	check.L("initial rsync done", "duration", since(start))

	watchLoop(watcher)
}

func watchDir(watcher *fsnotify.Watcher, dir string, recursive bool) {
	check.L("watching", "basedir", dir)
	check.E(watcher.Add(dir)).F("watcher.add")
	if !recursive {
		return
	}

	check.E(fs.WalkDir(os.DirFS(dir), ".", func(path string, de fs.DirEntry, err error) error {
		check.E(err).F("walkdir", "path", path)
		if de.IsDir() && de.Type()&fs.ModeSymlink == 0 &&
			!strings.HasPrefix(path, ".") && !strings.Contains(path, "/.") {
			path = filepath.Join(dir, path)
			check.L("watching", "dir", path)
			check.E(watcher.Add(path)).F("watcher.add")
		}
		return nil
	})).F("walkdir")
}

func since(t time.Time) time.Duration {
	return time.Now().Sub(t).Truncate(time.Millisecond)
}

const interestingOps = fsnotify.Create | fsnotify.Write | fsnotify.Remove

func watchLoop(watcher *fsnotify.Watcher) {
	for {
		select {
		case ev, ok := <-watcher.Events:
			check.T(ok).F("recv watcher event")
			start := time.Now()
			check.L("start cycle", "event", ev)
			evs := collectEvents(watcher, ev)
			processEvents(evs)
			check.L("finish cycle", "duration", since(start))
		}
	}
}

func collectEvents(watcher *fsnotify.Watcher, ev fsnotify.Event) []fsnotify.Event {
	var evs []fsnotify.Event
	if (ev.Op & interestingOps) != 0 {
		evs = append(evs, ev)
	}

	if eventDelayDuration > 0 {
		time.Sleep(eventDelayDuration)
	}

	for {
		select {
		case ev, ok := <-watcher.Events:
			check.T(ok).F("recv watcher event")
			if verbose {
				check.L("recv", "event", ev)
			}
			if (ev.Op & interestingOps) != 0 {
				evs = append(evs, ev)
			}
		default:
			return evs
		}
	}
}

type FileToSync struct {
	Name     string
	Order    int
	IsRemove bool
}

func processEvents(evs []fsnotify.Event) {
	// filename => FileToSync
	// only the last event matters
	// keep the event order
	filesToSyncMap := make(map[string]*FileToSync)
	for i, ev := range evs {
		if ignoreFile(ev.Name) {
			if verbose {
				check.L("ignore", "file", ev.Name)
			}
			continue
		}

		isrm := (ev.Op & fsnotify.Remove) != 0
		check.L("add", "file", ev.Name, "evop", ev.Op, "rm", isrm)
		filesToSyncMap[ev.Name] = &FileToSync{Name: ev.Name, Order: i, IsRemove: isrm}
	}
	var filesToSync []*FileToSync
	for _, fts := range filesToSyncMap {
		filesToSync = append(filesToSync, fts)
	}
	// no need to sort
	slices.SortFunc(filesToSync, func(a, b *FileToSync) int { return a.Order - b.Order })

	check.L("processevents", "num_events", len(evs), "num_files", len(filesToSync))

	processFiles(filesToSync)
}

func processFiles(filesToSync []*FileToSync) {
	var done sync.WaitGroup
	done.Add(parallel)

	ch := make(chan *FileToSync, parallel)
	for i := 0; i < parallel; i++ {
		go func() {
			defer done.Done()

			for fts := range ch {
				processFile(fts.Name, fts.IsRemove)
			}
		}()
	}

	for _, fts := range filesToSync {
		ch <- fts
	}
	close(ch)
	done.Wait()
}

var ignoredExts = []string{".o", ".so", ".exe", ".dylib", ".test", ".out"}

func ignoreFile(filename string) bool {
	if strings.Contains(filename, "/.") || strings.HasSuffix(filename, "~") {
		return true
	}
	ext := strings.ToLower(filepath.Ext(filename))
	if slices.Contains(ignoredExts, ext) {
		return true
	}
	if mode := mygo.FileMode(filename); mode&(os.ModeDir|os.ModeSymlink) != 0 {
		return true
	}
	if guessText && !mygo.GuessUTF8File(filename) {
		return true
	}
	return false
}

func rsync(src, dst string) {
	check.L("rsync", "src", src, "dst", dst)
	mygo.NewCmd("rsync", "-a", "-e", "ssh", src, dst).Run()
}

func processFile(filename string, isRemove bool) {
	p := strings.TrimPrefix(filename, baseDir)
	dst := fmt.Sprintf("%s%s", remotePath, p)
	if isRemove {
		removeRemoteFile(dst)
	} else {
		rsync(filename, dst)
	}
}

func removeRemoteFile(rsyncFilename string) {
	ss := strings.SplitN(rsyncFilename, ":", 2)
	check.T(len(ss) == 2).F("malformed rsync filename to remove", "file", rsyncFilename)
	check.L("ssh rm", "host", ss[0], "file", ss[1])
	mygo.NewCmd("ssh", ss[0], "rm", ss[1]).Run()
}
