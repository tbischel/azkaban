package main

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/hashicorp/raft"
)

const (
	raftTimeout      = 10 * time.Second
	InProgressAction = "in progress"
	CompletedAction  = "completed"
)

var (
	errNotLeader = errors.New("azkaban: not leader")
)

type command struct {
	Action     string
	Owner      string
	Resource   string
	ExpireTime int64
	Now        int64
}

type InProgressResponse struct {
	ExpireTime int64
	Error      error
}

type CompletedResponse struct {
	Error error
}

type Cellblock struct {
	RaftDir  string
	RaftBind string
	raft     *raft.Raft
	store    LockStorer
}

func NewCellblock() *Cellblock {
	return &Cellblock{
		store: NewLockStorer(1024 * 1024 * 20),
	}
}

func (cb *Cellblock) InProgressLock(resource, owner string, expire int64) (int64, error) {
	if cb.raft.State() != raft.Leader {
		return 0, errNotLeader
	}

	c := &command{
		Action:     InProgressAction,
		Resource:   resource,
		Owner:      owner,
		ExpireTime: expire,
		Now:        time.Now().Unix(),
	}

	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(c)
	if err != nil {
		return 0, err
	}
	b := buffer.Bytes()
	f := cb.raft.Apply(b, raftTimeout)
	err = f.Error()
	if err != nil {
		return 0, err
	}
	resp := f.Response()
	inProgressResponse := resp.(InProgressResponse)
	return inProgressResponse.ExpireTime, inProgressResponse.Error
}

func (cb *Cellblock) CompletedLock(resource, owner string, expire int64) error {
	if cb.raft.State() != raft.Leader {
		return errNotLeader
	}

	c := &command{
		Action:     CompletedAction,
		Resource:   resource,
		Owner:      owner,
		ExpireTime: expire,
		Now:        time.Now().Unix(),
	}

	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(c)
	if err != nil {
		return err
	}
	b := buffer.Bytes()
	f := cb.raft.Apply(b, raftTimeout)
	err = f.Error()
	if err != nil {
		return err
	}
	resp := f.Response()
	completedResponse := resp.(CompletedResponse)
	return completedResponse.Error
}

func (cb *Cellblock) Apply(l *raft.Log) interface{} {
	b := l.Data
	r := bytes.NewReader(b)
	dec := gob.NewDecoder(r)
	var c command
	err := dec.Decode(&c)
	if err != nil {
		panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
	}

	switch c.Action {
	case InProgressAction:
		return cb.applyInProgressLock(c.Resource, c.Owner, c.ExpireTime, c.Now)
	case CompletedAction:
		return cb.applyCompletedLock(c.Resource, c.Owner, c.ExpireTime, c.Now)
	default:
		panic(fmt.Sprintf("unrecognized command action: %s", c.Action))
	}
}

func (cb *Cellblock) applyInProgressLock(resource, owner string, expire, now int64) interface{} {
	exp, err := cb.store.InProgressLock(resource, owner, expire, now)
	return InProgressResponse{ExpireTime: exp, Error: err}
}

func (cb *Cellblock) applyCompletedLock(resource, owner string, expire, now int64) interface{} {
	err := cb.store.CompletedLock(resource, owner, expire, now)
	return CompletedResponse{Error: err}
}

func (cb *Cellblock) Snapshot() (raft.FSMSnapshot, error) {
	return &CellblockSnapshoter{}, nil
}

func (cb *Cellblock) Restore(rc io.ReadCloser) error {
	return nil
}

type CellblockSnapshoter struct {
}

func (cs *CellblockSnapshoter) Persist(sink raft.SnapshotSink) error {
	return nil
}

func (cs *CellblockSnapshoter) Release() {

}
