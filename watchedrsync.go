package main

import (
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/fsnotify/fsnotify"
	"github.com/zncoder/check"
	"github.com/zncoder/mygo"
)

var (
	baseDir    string
	remotePath string
	shallow    bool
	verbose    bool
)

func main() {
	flag.StringVar(&remotePath, "r", "", "remote parent dir in rsync format, host:dir")
	flag.BoolVar(&shallow, "s", false, "watch just local_dir, not subdirs")
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

	watcher := check.V(fsnotify.NewWatcher()).F("NewWatcher")
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
		if path != "." && de.IsDir() && de.Type()&fs.ModeSymlink == 0 {
			path = filepath.Join(dir, path)
			check.L("watching", "dir", path)
			check.E(watcher.Add(path)).F("watcher.add")
		}
		return nil
	})).F("walkdir")
}

func watchLoop(watcher *fsnotify.Watcher) {
	const ops = fsnotify.Create | fsnotify.Write
	for {
		select {
		case ev, ok := <-watcher.Events:
			check.T(ok).F("recv watcher event")
			// TODO: handle remove, rename by rsync the whole dir
			// TODO: handle create of dir
			if verbose {
				check.L("recv", "event", ev)
			}
			if (ev.Op & ops) == 0 {
				continue
			}
			ignored := ignoreFile(ev.Name)
			if !ignored {
				processEvent(ev.Name)
			} else if verbose {
				check.L("ignore", "filename", ev.Name)
			}
		}
	}
}

var ignoredExts = []string{".o", ".so", ".exe", ".dylib", ".test", ".out"}

func ignoreFile(filename string) bool {
	base := filepath.Base(filename)
	if strings.HasPrefix(base, ".") || strings.HasSuffix(base, "~") {
		return true
	}
	ext := strings.ToLower(filepath.Ext(filename))
	if slices.Contains(ignoredExts, ext) {
		return true
	}
	if mygo.IsSymlink(filename) {
		return true
	}
	if !mygo.GuessUTF8File(filename) {
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
