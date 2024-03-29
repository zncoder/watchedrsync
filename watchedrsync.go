package main

import (
	"flag"
	"fmt"
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
	verbose    bool
)

func main() {
	flag.StringVar(&remotePath, "r", "", "remote path in rsync format, e.g. foo:bar")
	flag.BoolVar(&verbose, "v", false, "verbose")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s <local_dir>\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()
	check.T(remotePath != "").F("no remotedir")
	check.T(flag.NArg() == 1).F("no dir")
	baseDir = flag.Arg(0)
	baseDir = check.V(filepath.Abs(baseDir)).F("invalid dir", "dir", baseDir) + "/"
	remotePath = filepath.Clean(remotePath) + "/"
	check.L("sync to remote", "dir", remotePath)

	watcher := check.V(fsnotify.NewWatcher()).F("NewWatcher")
	check.E(watcher.Add(baseDir))
	check.L("watching", "dir", baseDir)

	rsync(baseDir, remotePath)

	watchLoop(watcher)
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
