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
	verbose            bool
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	flag.StringVar(&remotePath, "r", "", "remote parent dir in rsync format, host:dir")
	flag.BoolVar(&shallow, "s", false, "watch just local_dir, not subdirs")
	flag.BoolVar(&guessText, "t", false, "guess file content is text")
	flag.DurationVar(&eventDelayDuration, "d", 2*time.Second, "delay to batch processing events")
	flag.BoolVar(&verbose, "v", false, "verbose")
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

	rsync(baseDir, remotePath)

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

const interestingOps = fsnotify.Create | fsnotify.Write

func watchLoop(watcher *fsnotify.Watcher) {
	for {
		select {
		case ev, ok := <-watcher.Events:
			check.T(ok).F("recv watcher event")
			if verbose {
				check.L("recv", "event", ev)
			}
			evs := collectEvents(watcher, ev)
			processEvents(evs)
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

func processEvents(evs []fsnotify.Event) {
	check.L("processevents", "num", len(evs))
	for _, ev := range evs {
		// TODO: handle remove, rename by rsync the whole dir
		// TODO: handle create of dir
		ignored := ignoreFile(ev.Name)
		if !ignored {
			processEvent(ev.Name)
		} else if verbose {
			check.L("ignore", "filename", ev.Name)
		}
	}
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
	if mygo.IsSymlink(filename) {
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

func processEvent(filename string) {
	p := strings.TrimPrefix(filename, baseDir)
	dst := fmt.Sprintf("%s%s", remotePath, p)
	rsync(filename, dst)
}
