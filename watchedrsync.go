package main

import (
	"flag"
	"fmt"
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
	flag.Parse()
	check.T(remotePath != "").F("no remotedir")
	check.T(flag.NArg() == 1).F("no dir")
	check.L("sync to remote", "dir", remotePath)
	remotePath = filepath.Clean(remotePath)

	watcher := check.V(fsnotify.NewWatcher()).F("NewWatcher")

	go watchLoop(watcher)

	baseDir = flag.Arg(0)
	baseDir = check.V(filepath.Abs(baseDir)).F("invalid dir", "dir", baseDir)
	check.E(watcher.Add(baseDir))
	check.L("watching", "dir", baseDir)

	select {}
}

func watchLoop(watcher *fsnotify.Watcher) {
	for {
		select {
		case ev, ok := <-watcher.Events:
			check.T(ok).F("recv watcher event")
			// TODO: process async
			// TODO: handle remove, rename by rsync the whole dir
			// TODO: handle create of dir
			if ev.Op == fsnotify.Create || ev.Op == fsnotify.Write {
				processEvent(ev.Name)
			}
		}
	}
}

func processEvent(filename string) {
	p := strings.TrimPrefix(filename, baseDir)
	dest := fmt.Sprintf("%s/%s", remotePath, p)
	check.L("rsync", "filename", filename, "dest", dest)
	mygo.NewCmd("rsync", "-a", filename, dest).Run()
}
