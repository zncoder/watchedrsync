package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/fsnotify/fsnotify"
	"github.com/zncoder/check"
	"github.com/zncoder/mygo"
)

var (
	baseDir    string
	remotePath string
)

func main() {
	flag.StringVar(&remotePath, "r", "", "remote path in rsync format, e.g. foo:bar")
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
	for {
		select {
		case ev, ok := <-watcher.Events:
			check.T(ok).F("recv watcher event")
			// TODO: process async
			// TODO: handle remove, rename by rsync the whole dir
			// TODO: handle create of dir
			// TODO: ignore files
			if ev.Op == fsnotify.Create || ev.Op == fsnotify.Write {
				processEvent(ev.Name)
			}
		}
	}
}

func rsync(src, dst string) {
	check.L("rsync", "src", src, "dst", dst)
	mygo.NewCmd("rsync", "-a", "-e", "ssh", "--delete", src, dst).Run()
}

func processEvent(filename string) {
	p := strings.TrimPrefix(filename, baseDir)
	dst := fmt.Sprintf("%s%s", remotePath, p)
	rsync(filename, dst)
}
