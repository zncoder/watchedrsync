package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
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
	guessText          = flag.Bool("t", false, "guess file content is text")
	parallel           = flag.Int("p", 10, "parallelism")
	eventDelayDuration = flag.Duration("d", 2*time.Second, "delay to batch processing events")
	sockAddr           = flag.String("s", os.ExpandEnv("$HOME/.cache/watchedrsync.sock"), "unix socket addr")
	verbose            = flag.Bool("v", true, "verbose")
)

var watchedDirs = make(map[string]string)

func watchDir(watcher *fsnotify.Watcher, local, remote string) error {
	check.T(strings.HasSuffix(local, "/")).F("local dir must end with /", "local", local)
	check.T(strings.HasSuffix(remote, "/")).F("remote dir must end with /", "remote", remote)

	for ld, rd := range watchedDirs {
		if local == ld || remote == rd {
			return fmt.Errorf("already watching %q => %q", ld, rd)
		}
	}

	check.L("watching", "local", local, "remote", remote)
	check.E(watcher.Add(local)).F("watcher.add", "local", local)
	watchedDirs[local] = remote
	return nil
}

type Op struct{}

func (Op) Daemon() {
	mygo.ParseFlag("local-dir")

	os.Remove(*sockAddr)
	check.L("listening", "sock", *sockAddr)
	lr := check.V(net.Listen("unix", *sockAddr)).F("listen")
	defer lr.Close()

	watcher := check.V(fsnotify.NewBufferedWatcher(50)).F("NewWatcher")
	go watchLoop(watcher)

	buf := make([]byte, 1024*1024)
	for {
		conn, err := lr.Accept()
		if err != nil {
			check.L("accept failed", "err", err)
			continue
		}
		handleConn(conn, watcher, buf)
	}
}

type WatchedDir struct {
	LocalDir  string // dir/
	RemoteDir string // host:dir/
}

func NewWatchedDir(buf []byte) (wd WatchedDir, err error) {
	err = json.Unmarshal(buf, &wd)
	if err != nil {
		return wd, fmt.Errorf("unmarshal err:%w", err)
	}

	dir, err := filepath.Abs(wd.LocalDir)
	if err != nil {
		return wd, fmt.Errorf("abs localdir:%q err:%w", wd.LocalDir, err)
	}
	wd.LocalDir = dir + "/"
	if !mygo.IsDir(wd.LocalDir) {
		return wd, fmt.Errorf("localdir:%q is not a dir", wd.LocalDir)
	}
	wd.RemoteDir = filepath.Clean(wd.RemoteDir) + "/"
	return wd, nil
}

type JsonResponse struct {
	Ok  string `json:"ok",omitempty`
	Err string `json:"err",omitempty`
}

func handleConn(conn net.Conn, watcher *fsnotify.Watcher, buf []byte) {
	defer conn.Close()

	var err error
	defer func() {
		if err != nil {
			jr := JsonResponse{Err: err.Error()}
			json.NewEncoder(conn).Encode(&jr)
		}
	}()

	n, err := conn.Read(buf)
	if err != nil {
		err = fmt.Errorf("read input err:%w", err)
		return
	}
	wd, err := NewWatchedDir(buf[:n])
	if err != nil {
		return
	}
	err = watchDir(watcher, wd.LocalDir, wd.RemoteDir)
	if err != nil {
		return
	}
	jr := JsonResponse{Ok: fmt.Sprintf("watching %q => %q", wd.LocalDir, wd.RemoteDir)}
	json.NewEncoder(conn).Encode(&jr)
}

func (Op) Add() {
	mygo.ParseFlag("local remote")
	check.T(flag.NArg() == 2).F("need local and remote dir")
	wd := WatchedDir{LocalDir: flag.Arg(0), RemoteDir: flag.Arg(1)}

	conn := check.V(net.Dial("unix", *sockAddr)).F("dial", "sock", *sockAddr)
	defer conn.Close()

	check.E(json.NewEncoder(conn).Encode(&wd)).F("write request", "wd", wd)
	var jr JsonResponse
	check.E(json.NewDecoder(conn).Decode(&jr)).F("read response")
	check.T(jr.Err == "").F("add failed", "err", jr.Err)
	check.L(jr.Ok)
}

func (Op) RM_Remove() {

}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	mygo.RunOpMapCmd[Op]()
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

	deadlineC := time.After(*eventDelayDuration)

	for {
		select {
		case ev, ok := <-watcher.Events:
			check.T(ok).F("recv watcher event")
			if *verbose {
				check.L("recv", "event", ev)
			}
			if (ev.Op & interestingOps) != 0 {
				evs = append(evs, ev)
			}
		case <-time.After(500 * time.Millisecond):
			return evs
		case <-deadlineC:
			return evs
		}
	}
}

type FileToSync struct {
	Name     string
	Order    int
	IsRemove bool
}

// make it global to support retry
var filesToSyncMap = make(map[string]*FileToSync)

func processEvents(evs []fsnotify.Event) {
	// filename => FileToSync
	// only the last event matters
	// keep the event order
	for i, ev := range evs {
		if ignoreFile(ev.Name) {
			if *verbose {
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

	if err := processFiles(filesToSync); err != nil {
		check.L("processfiles failed", "err", err)
		return
	}
	filesToSyncMap = make(map[string]*FileToSync)
}

func processFiles(filesToSync []*FileToSync) error {
	var done sync.WaitGroup
	done.Add(*parallel)

	errCh := make(chan error, 1)
	ch := make(chan *FileToSync, *parallel)
	for i := 0; i < *parallel; i++ {
		go func() {
			defer done.Done()

			for fts := range ch {
				if err := processFile(fts.Name, fts.IsRemove); err != nil {
					select {
					case errCh <- err:
					default:
					}
				}
			}
		}()
	}

	for _, fts := range filesToSync {
		ch <- fts
	}
	close(ch)
	done.Wait()

	select {
	case err := <-errCh:
		return err
	default:
		return nil
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
	if mode := mygo.FileMode(filename); mode&(os.ModeDir|os.ModeSymlink) != 0 {
		return true
	}
	if *guessText && !mygo.GuessUTF8File(filename) {
		return true
	}
	return false
}

func rsync(src, dst string) error {
	check.L("rsync", "src", src, "dst", dst)
	return mygo.NewCmd("rsync", "-a", "-e", "ssh", src, dst).C.Run()
}

func processFile(filename string, isRemove bool) (err error) {
	dir, p := filepath.Split(filename)
	remotePath := watchedDirs[dir]
	check.T(remotePath != "").F("no remote dir", "dir", dir, "filename", p)
	dst := fmt.Sprintf("%s%s", remotePath, p)
	if isRemove {
		err = removeRemoteFile(dst)
	} else {
		err = rsync(filename, dst)
	}
	return err
}

func removeRemoteFile(rsyncFilename string) error {
	ss := strings.SplitN(rsyncFilename, ":", 2)
	check.T(len(ss) == 2).F("malformed rsync filename to remove", "file", rsyncFilename)
	check.L("ssh rm", "host", ss[0], "file", ss[1])
	return mygo.NewCmd("ssh", ss[0], "rm", ss[1]).C.Run()
}
