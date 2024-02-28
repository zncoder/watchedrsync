package main

import (
	"flag"
	"path/filepath"

	"github.com/fsnotify/fsnotify"
	"github.com/zncoder/check"
)

func main() {
	remoteDir := flag.String("r", "", "remote dir in rsync format, e.g. foo:bar/")
	flag.Parse()
	check.T(*remoteDir != "").F("no remotedir")
	check.T(flag.NArg() > 0).F("no dir")
	check.L("sync to remote", "dir", *remoteDir)

	watcher := check.V(fsnotify.NewWatcher()).F("NewWatcher")
	go watchLoop(watcher)

	for _, dir := range flag.Args() {
		dir = check.V(filepath.Abs(dir)).F("invalid dir", "dir", dir)
		check.E(watcher.Add(dir))
		check.L("watching", "dir", dir)
	}
	select {}
}

func watchLoop(watcher *fsnotify.Watcher) {
	for {
		select {
		case ev, ok := <-watcher.Events:
			check.T(ok).F("recv watcher event")
			check.L("received", "event", ev)
		}
	}
}
