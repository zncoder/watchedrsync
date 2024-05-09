package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/fs"
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

type WatchDirArg struct {
	LocalDir  string // dir/
	RemoteDir string // host:dir/
}

func validateLocalDir(local string) (string, error) {
	if !filepath.IsAbs(local) {
		return "", fmt.Errorf("localdir:%q is not abs", local)
	}
	local, err := filepath.Abs(local)
	if err != nil {
		return "", fmt.Errorf("abs localdir:%q err:%w", local, err)
	}
	local += "/"
	if !mygo.IsDir(local) {
		return "", fmt.Errorf("localdir:%q is not a dir", local)
	}
	return local, nil
}

func validateWatchDir(arg *WatchDirArg) (local, remote string, err error) {
	local, remote = arg.LocalDir, arg.RemoteDir
	local, err = validateLocalDir(local)
	if err != nil {
		return "", "", err
	}

	remote = filepath.Clean(remote)
	ss := strings.Split(remote, ":")
	if len(ss) != 2 || ss[0] == "" || ss[1] == "" {
		return "", "", fmt.Errorf("remote dir must be host:dir")
	}
	if ss[1] == "-" {
		home := check.V(os.UserHomeDir()).F("no userhomedir") + "/"
		if strings.HasPrefix(local, home) {
			remote = ss[0] + ":" + strings.TrimPrefix(local, home)
		} else {
			remote = ss[0] + ":" + local
		}
	}
	remote = filepath.Clean(remote) + "/"
	return local, remote, nil
}

func (Op) Add() {
	mygo.ParseFlag("local remotehost:remotedir_or_-")
	ld := check.V(filepath.Abs(flag.Arg(0))).F("abs", "local", flag.Arg(0))
	call(&JsonArg{WatchDir: &WatchDirArg{LocalDir: ld, RemoteDir: flag.Arg(1)}})
}

func call(arg *JsonArg) {
	conn := check.V(net.Dial("unix", *sockAddr)).F("dial", "sock", *sockAddr)
	defer conn.Close()

	check.E(json.NewEncoder(conn).Encode(arg)).F("write request", "arg", arg)
	var jr JsonResult
	check.E(json.NewDecoder(conn).Decode(&jr)).F("read response")
	check.T(jr.Err == "").F("call failed", "err", jr.Err, "arg", arg)
	fmt.Println(jr.Ok)
}

func (Op) RM_Remove() {
	mygo.ParseFlag("local")
	ld := check.V(filepath.Abs(flag.Arg(0))).F("abs", "local", flag.Arg(0))
	call(&JsonArg{RemoveDir: ld})
}

func (Op) LS_List() {
	mygo.ParseFlag()
	call(&JsonArg{ListWatched: true})
}

func (Op) QuitQuitQuit() {
	mygo.ParseFlag()
	call(&JsonArg{QuitQuitQuit: true})
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
	quitting           bool
}

func (dm *Daemon) RequestLoop(lr net.Listener) {
	buf := make([]byte, 1024*1024)
	for !dm.quitting {
		if conn, ok := check.V(lr.Accept()).L("accept"); ok {
			dm.handleConn(conn, buf)
		}
	}
}

type JsonArg struct {
	WatchDir     *WatchDirArg `json:"watchdir",omitempty`
	RemoveDir    string       `json:"removedir",omitempty`
	ListWatched  bool         `json:"listwatched",omitempty`
	QuitQuitQuit bool         `json:"quitquitquit",omitempty`
}

type JsonResult struct {
	Ok  string `json:"ok",omitempty`
	Err string `json:"err",omitempty`
}

func (dm *Daemon) handleConn(conn net.Conn, buf []byte) {
	defer conn.Close()

	var ok string
	var err error
	defer func() {
		if err != nil {
			json.NewEncoder(conn).Encode(&JsonResult{Err: err.Error()})
		} else {
			json.NewEncoder(conn).Encode(&JsonResult{Ok: ok})
		}
	}()

	var ja JsonArg
	if err = json.NewDecoder(conn).Decode(&ja); err != nil {
		return
	}
	if ja.WatchDir != nil {
		ok, err = dm.doWatch(ja.WatchDir)
	}
	if ja.RemoveDir != "" {
		ok, err = dm.doRemove(ja.RemoveDir)
	}
	if ja.ListWatched {
		ok, err = dm.doList()
	}
	if ja.QuitQuitQuit {
		dm.quitting = true
		ok = "quitting"
	}
}

func (dm *Daemon) doWatch(wd *WatchDirArg) (string, error) {
	local, remote, err := validateWatchDir(wd)
	if err != nil {
		return "", err
	}

	if err = dm.watchDir(local, remote); err != nil {
		return "", err
	}
	return fmt.Sprintf("watching %q => %q", local, remote), nil
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
	if err := rsyncMkdir(remote); err != nil {
		return err
	}
	check.E(dm.watcher.Add(local)).F("watcher.add", "local", local)
	dm.watchedDirs[local] = remote
	dm.syncDir(local)
	return nil
}

func (dm *Daemon) doRemove(local string) (string, error) {
	local, err := validateLocalDir(local)
	if err != nil {
		return "", err
	}
	remote, ok := dm.watchedDirs[local]
	if !ok {
		return "", fmt.Errorf("dir:%q not found", local)
	}
	check.E(dm.watcher.Remove(local)).F("remove dir from watcher", "local", local)
	delete(dm.watchedDirs, local)
	return fmt.Sprintf("removed %q => %q", local, remote), nil
}

func (dm *Daemon) doList() (string, error) {
	var sb strings.Builder
	for local, remote := range dm.watchedDirs {
		fmt.Fprintf(&sb, "%q => %q\n", local, remote)
	}
	return sb.String(), nil
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

func rsyncFile(src, dst string) error {
	check.L("rsync", "src", src, "dst", dst)
	return mygo.NewCmd("rsync", "-t", "-e", "ssh", src, dst).C.Run()
}

func (dm *Daemon) syncDir(local string) {
	remote := dm.watchedDirs[local]
	check.T(remote != "").F("no remote dir", "local", local)

	localfs := os.DirFS(local)
	des := check.V(fs.ReadDir(localfs, ".")).F("readdir", "dir", local)
	for _, de := range des {
		if de.Type().IsRegular() {
			check.E(dm.processFile(filepath.Join(local, de.Name()), false)).F("syncdir", "local", local, "file", de.Name())
		}
	}
}

func (dm *Daemon) processFile(filename string, isRemove bool) (err error) {
	dir, p := filepath.Split(filename)
	remote := dm.watchedDirs[dir]
	check.T(remote != "").F("no remote dir", "dir", dir, "filename", p)
	dst := fmt.Sprintf("%s%s", remote, p)
	if isRemove {
		err = rsyncRemoveFile(dst)
	} else {
		err = rsyncFile(filename, dst)
	}
	return err
}

func rsyncRemoveFile(rsyncFilename string) error {
	ss := strings.SplitN(rsyncFilename, ":", 2)
	check.T(len(ss) == 2).F("malformed rsync filename to remove", "file", rsyncFilename)
	check.L("ssh rm", "host", ss[0], "file", ss[1])
	if err := mygo.NewCmd("ssh", ss[0], "rm", ss[1]).C.Run(); err != nil {
		return fmt.Errorf("ssh rm %q failed err:%w", rsyncFilename, err)
	}
	return nil
}

func rsyncMkdir(remote string) error {
	ss := strings.SplitN(remote, ":", 2)
	check.T(len(ss) == 2).F("malformed rsync filename to remove", "file", remote)
	check.L("ssh mkdir", "host", ss[0], "file", ss[1])
	if err := mygo.NewCmd("ssh", ss[0], "mkdir", "-p", ss[1]).C.Run(); err != nil {
		return fmt.Errorf("ssh mkdir -p %q failed err:%w", remote, err)
	}
	return nil
}
