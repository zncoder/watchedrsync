package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
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
	sockAddr = flag.String("s", os.ExpandEnv("$HOME/.cache/watchedrsync.sock"), "unix socket addr")
	verbose  = flag.Bool("v", true, "verbose")
)

type Op struct{}

func (Op) SD_StartDaemon() {
	guessText := flag.Bool("t", false, "guess file content is text")
	parallel := flag.Int("p", 10, "parallelism")
	eventDelayDuration := flag.Duration("d", 2*time.Second, "delay to batch processing events")
	mygo.ParseFlag()

	os.Remove(*sockAddr)
	check.L("listening", "sock", *sockAddr)
	lr := check.V(net.Listen("unix", *sockAddr)).F("listen")
	defer lr.Close()

	dm := &Daemon{
		watchedDirs:        make(map[string]string),
		filesToSyncMap:     make(map[string]*FileToSync),
		watcher:            check.V(fsnotify.NewBufferedWatcher(50)).F("NewWatcher"),
		guessText:          *guessText,
		parallel:           *parallel,
		eventDelayDuration: *eventDelayDuration,
	}
	go dm.WatchLoop()
	dm.RequestLoop(lr)
}

type WatchedDir struct {
	LocalDir  string // dir/
	RemoteDir string // host:dir/
}

func (wd *WatchedDir) Validate() error {
	if !filepath.IsAbs(wd.LocalDir) {
		return fmt.Errorf("localdir:%q is not abs", wd.LocalDir)
	}
	dir, err := filepath.Abs(wd.LocalDir)
	if err != nil {
		return fmt.Errorf("abs localdir:%q err:%w", wd.LocalDir, err)
	}
	wd.LocalDir = dir + "/"
	if !mygo.IsDir(wd.LocalDir) {
		return fmt.Errorf("localdir:%q is not a dir", wd.LocalDir)
	}
	wd.RemoteDir = filepath.Clean(wd.RemoteDir) + "/"
	return nil
}

func readArg(r io.Reader, arg any, buf []byte) error {
	n, err := r.Read(buf)
	if err != nil {
		return fmt.Errorf("read arg:%T err:%w", arg, err)
	}
	err = json.Unmarshal(buf[:n], arg)
	if err != nil {
		return fmt.Errorf("unmarshal arg:%T err:%w", arg, err)
	}
	return nil
}

func writeResult(w io.Writer, result any) error {
	err := json.NewEncoder(w).Encode(result)
	if err != nil {
		return fmt.Errorf("write result:%T err:%w", result, err)
	}
	return nil
}

func (Op) Add() {
	mygo.ParseFlag("local remote")
	check.T(flag.NArg() == 2).F("need local and remote dir")
	ld := check.V(filepath.Abs(flag.Arg(0))).F("abs", "local", flag.Arg(0))
	wd := WatchedDir{LocalDir: ld, RemoteDir: flag.Arg(1)}

	conn := check.V(net.Dial("unix", *sockAddr)).F("dial", "sock", *sockAddr)
	defer conn.Close()

	check.E(writeResult(conn, wd)).F("write request", "wd", wd)
	var jr JsonResponse
	check.E(readArg(conn, &jr, make([]byte, 1024*1024))).F("read response")
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

type Daemon struct {
	watchedDirs        map[string]string
	filesToSyncMap     map[string]*FileToSync
	watcher            *fsnotify.Watcher
	guessText          bool
	parallel           int
	eventDelayDuration time.Duration
}

func (dm *Daemon) RequestLoop(lr net.Listener) {
	buf := make([]byte, 1024*1024)
	for {
		if conn, ok := check.V(lr.Accept()).L("accept"); ok {
			dm.handleConn(conn, buf)
		}
	}
}

type JsonResponse struct {
	Ok  string `json:"ok",omitempty`
	Err string `json:"err",omitempty`
}

func (dm *Daemon) handleConn(conn net.Conn, buf []byte) {
	defer conn.Close()

	var err error
	defer func() {
		if err != nil {
			jr := JsonResponse{Err: err.Error()}
			json.NewEncoder(conn).Encode(&jr)
		}
	}()

	var wd WatchedDir
	if err = readArg(conn, &wd, buf); err != nil {
		return
	}
	if err = wd.Validate(); err != nil {
		return
	}

	err = dm.watchDir(wd.LocalDir, wd.RemoteDir)
	if err != nil {
		return
	}
	jr := JsonResponse{Ok: fmt.Sprintf("watching %q => %q", wd.LocalDir, wd.RemoteDir)}
	check.E(writeResult(conn, &jr)).L("write response")
}

func (dm *Daemon) watchDir(local, remote string) error {
	check.T(strings.HasSuffix(local, "/")).F("local dir must end with /", "local", local)
	check.T(strings.HasSuffix(remote, "/")).F("remote dir must end with /", "remote", remote)

	for ld, rd := range dm.watchedDirs {
		if local == ld || remote == rd {
			return fmt.Errorf("already watching %q => %q", ld, rd)
		}
	}

	check.L("watching", "local", local, "remote", remote)
	check.E(dm.watcher.Add(local)).F("watcher.add", "local", local)
	dm.watchedDirs[local] = remote
	return nil
}

func (dm *Daemon) WatchLoop() {
	for {
		select {
		case ev, ok := <-dm.watcher.Events:
			check.T(ok).F("recv watcher event")
			start := time.Now()
			check.L("start cycle", "event", ev)
			evs := dm.collectEvents(ev)
			dm.processEvents(evs)
			check.L("finish cycle", "duration", since(start))
		}
	}
}

func (dm *Daemon) collectEvents(ev fsnotify.Event) []fsnotify.Event {
	var evs []fsnotify.Event
	if (ev.Op & interestingOps) != 0 {
		evs = append(evs, ev)
	}

	deadlineC := time.After(dm.eventDelayDuration)

	for {
		select {
		case ev, ok := <-dm.watcher.Events:
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

func (dm *Daemon) processEvents(evs []fsnotify.Event) {
	// filename => FileToSync
	// only the last event matters
	// keep the event order
	for i, ev := range evs {
		if dm.ignoreFile(ev.Name) {
			if *verbose {
				check.L("ignore", "file", ev.Name)
			}
			continue
		}

		isrm := (ev.Op & fsnotify.Remove) != 0
		check.L("add", "file", ev.Name, "evop", ev.Op, "rm", isrm)
		dm.filesToSyncMap[ev.Name] = &FileToSync{Name: ev.Name, Order: i, IsRemove: isrm}
	}

	var filesToSync []*FileToSync
	for _, fts := range dm.filesToSyncMap {
		filesToSync = append(filesToSync, fts)
	}
	// no need to sort
	slices.SortFunc(filesToSync, func(a, b *FileToSync) int { return a.Order - b.Order })

	check.L("processevents", "num_events", len(evs), "num_files", len(filesToSync))

	if err := dm.processFiles(filesToSync); err != nil {
		check.L("processfiles failed", "err", err)
		return
	}
	dm.filesToSyncMap = make(map[string]*FileToSync)
}

func (dm *Daemon) processFiles(filesToSync []*FileToSync) error {
	var done sync.WaitGroup
	done.Add(dm.parallel)

	errCh := make(chan error, 1)
	ch := make(chan *FileToSync, dm.parallel)
	for i := 0; i < dm.parallel; i++ {
		go func() {
			defer done.Done()

			for fts := range ch {
				if err := dm.processFile(fts.Name, fts.IsRemove); err != nil {
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

func (dm *Daemon) ignoreFile(filename string) bool {
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
	if dm.guessText && !mygo.GuessUTF8File(filename) {
		return true
	}
	return false
}

func rsync(src, dst string) error {
	check.L("rsync", "src", src, "dst", dst)
	return mygo.NewCmd("rsync", "-a", "-e", "ssh", src, dst).C.Run()
}

func (dm *Daemon) processFile(filename string, isRemove bool) (err error) {
	dir, p := filepath.Split(filename)
	remotePath := dm.watchedDirs[dir]
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
