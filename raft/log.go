// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	firstIndex, _ := storage.FirstIndex()
	lastIndex, _ := storage.LastIndex()
	ents, _  := storage.Entries(firstIndex, lastIndex+1)
	// TODO: 到时候修改其他的字段
	return &RaftLog{
		storage:         storage,
		entries:         ents,
		stabled:         lastIndex,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	ents := make([]pb.Entry, 0)
	for i := l.stabled+1; i <= l.LastIndex() ;i++{
		ents = append(ents, *l.GetEntry(i))
	}
	return ents
}


// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	//log.Infof("nextEnts Called, lastIndex: %d, applied: %d, committed: %d", l.LastIndex(), l.applied, l.committed)
	ent := make([]pb.Entry, 0)
	for i := l.applied+1; i <= l.committed; i++ {
		ent = append(ent, *l.GetEntry(i))
	}
	return ent
	//return l.entries[l.applied+1-l.entries[0].Index:l.committed+1-l.entries[0].Index]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		lastIndex, _ :=  l.storage.LastIndex()
		return lastIndex
	}
	return l.entries[len(l.entries)-1].Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if len(l.entries) == 0 || i > l.LastIndex(){
		return l.storage.Term(i)
	} else {
		return l.GetEntry(i).Term, nil
	}
}

func (l * RaftLog) LastTerm() uint64 {
	term, _ := l.Term(l.LastIndex())
	return term
}

func (l * RaftLog) AppendEntry(ent *pb.Entry) {
	l.entries = append(l.entries, *ent)
}

func (l * RaftLog) AppendEntries(ents []*pb.Entry) {
	for _, ent := range ents {
		l.AppendEntry(ent)
	}
}

func (l * RaftLog) GetEntry(i uint64) *pb.Entry {

	if len(l.entries) == 0 || l.entries[0].Index > i || i == 0 {
		return &pb.Entry{Term: 0, Index: l.LastIndex()}
	} else {
		//log.Infof("try to get log of %d ", i)
		//log.Infof("current logs: %+v", l.entries)
		return &l.entries[i - l.entries[0].Index]
	}
}