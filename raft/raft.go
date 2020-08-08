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
	"errors"
	"github.com/gogo/protobuf/sortkeys"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

// 选举超时的随机改变系数
const randomFactor = 0.3

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int // 心跳超时时间，超过这个时间就会尝试选举
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int // leader每隔Heartbeat时间就会发送一个包来确认自己的权威

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

// 用于表征每一个server
type Raft struct {
	id uint64 // 该server的id

	Term uint64 // 该server所在的Term
	Vote uint64 // 该server想要将票投给谁

	// the log
	RaftLog *RaftLog // log entries

	// log replication progress of each peers
	Prs map[uint64]*Progress // 保存的其他server的进度

	// this peer's role
	State StateType // 当前server的角色，包括follower,leader以及candidate

	// votes records
	votes map[uint64]bool // 其他的server对该server的投票结果（有点奇怪如何表示）

	// msgs need to send
	msgs []pb.Message // 构建一次RPC消息

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// number of ticks since it reached last electionTimeout
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).

	raftLog := newLog(c.Storage)
	lastIndex := raftLog.LastIndex()

	Prs := map[uint64]*Progress{}
	for _, peerId := range c.peers {
		Prs[peerId] = &Progress{
			Match: 0,
			Next:  lastIndex + 1,
		}
	}

	state, _, _ := raftLog.storage.InitialState()
	raftLog.committed = state.Commit
	Prs[c.ID].Match = lastIndex // 节点本身的match单独设置
	return &Raft{
		id:               c.ID,
		Term:             state.Term,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		State:            StateFollower, // 起始状态默认为follower
		Prs:              Prs,
		RaftLog:          raftLog,
		Vote:             state.Vote,
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).

	preIndex := r.Prs[to].Next - 1
	preLogTerm := uint64(0)
	if preIndex == 0 {
		preLogTerm = 0
	} else {
		preLogTerm, _ = r.RaftLog.Term(preIndex)
	}
	lastIndex := r.RaftLog.LastIndex()
	//if preIndex >= lastIndex {
	//	return false// 不需要发送新的
	//}
	var ents []*pb.Entry
	// 不包括preIndex的数据
	for i := preIndex + 1; i <= lastIndex; i++ {
		ents = append(ents, r.RaftLog.GetEntry(i))
	}

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: preLogTerm,
		Index:   preIndex,
		Entries: ents,
		Commit:  r.RaftLog.committed,
	})
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateLeader:
		// 如果是leader
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgBeat,
				To:      r.id,
				From:    r.id,
			})

		}
		//请问raft_paper_test里面的testNonleaderStartElection测试里面，因为超时了两次
	case StateCandidate, StateFollower:
		r.electionElapsed++
		if r.electionElapsed >= r.electionTimeout {
			//如果选举超时，写入
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgHup,
				To:      r.id,
				From:    r.id,
			})
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.Term = term
	r.Lead = lead
	r.Vote = None
	r.State = StateFollower
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	// 如果已经是candidate
	r.State = StateCandidate
	r.Term++
}

// 开启一轮新的投票
func (r *Raft) startNewRoundVoteRequest() {
	// 投票给自己

	r.Vote = r.id
	r.votes = map[uint64]bool{}
	r.votes[r.id] = true // TODO: 其实这里很奇怪，到底需不需要修改Vote字段，还是只需要修改votes的map即可
	// 特殊情况
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}
	// 随机化选举超时时间
	//rand.Seed(time.Now().Unix())
	//r.electionElapsed = -rand.Intn(int(randomFactor * float64(r.electionTimeout))) - 1
	r.electionElapsed = -rand.Intn(r.electionTimeout) - 1 // 有点不太懂

	// 请求投票
	//r.electionElapsed = 0
	for peerId := range r.Prs {
		if peerId != r.id {
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVote,
				To:      peerId,
				From:    r.id,
				Term:    r.Term,
				LogTerm: r.RaftLog.LastTerm(),
				Index:   r.RaftLog.LastIndex(),
			})
			// TODO: 此处应该添加Entries等字段
		}
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term

	r.State = StateLeader
	// 添加一个空的
	r.RaftLog.AppendEntry(&pb.Entry{
		Term:  r.Term,
		Index: r.RaftLog.LastIndex() + 1,
		Data:  nil,
	})
	// 添加完成之后需要发出请求
	for k := range r.Prs {
		if k != r.id {
			r.sendAppend(k)
		}
	}
	// 更新自身的Match以及Next
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1
	//r.RaftLog.stabled = r.RaftLog.LastIndex() // TODO: 搞清楚
}

// 比较日志，返回是否可以投票
func (r *Raft) allowVote(m pb.Message) bool {
	if r.Term == m.Term && r.Vote != m.From && r.Vote != None {
		return false
	}

	lastTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
	if m.LogTerm > lastTerm || (m.LogTerm == lastTerm && m.Index >= r.RaftLog.LastIndex()) {
		r.Vote = m.From
		return true
	}
	return false
}

// 尝试更新commitIndex
func (r *Raft) updateCommit() bool {
	var list []uint64
	for i, v := range r.Prs {
		if i == r.id {
			v.Match = r.RaftLog.LastIndex()
		}
		list = append(list, v.Match)
	}

	sortkeys.Uint64s(list)
	midMatch := list[(len(list)-1)/2]
	midTerm, _ := r.RaftLog.Term(midMatch)
	if midMatch > r.RaftLog.committed && midTerm == r.Term {
		r.RaftLog.committed = midMatch
		return true
	}
	return false
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		// 如果接收者是一个follower

		switch m.MsgType {
		case pb.MessageType_MsgHeartbeat:
			if m.Term >= r.Term {
				m.Term = max(r.Term, m.Term)
				r.Lead = m.From
				r.electionElapsed = -rand.Intn(r.electionTimeout) - 1
				// TODO: 暂时不回复消息
			}

		case pb.MessageType_MsgRequestVote:
			if m.Term > r.Term {
				r.Vote = None
				r.Term = m.Term
			}
			if m.Term < r.Term {
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgRequestVoteResponse,
					To:      m.From,
					From:    r.id,
					Term:    r.Term,
					Reject:  true,
				})
			} else {
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgRequestVoteResponse,
					To:      m.From,
					From:    r.id,
					Term:    r.Term,
					Reject:  !r.allowVote(m),
				})
			}

		case pb.MessageType_MsgAppend:
			if m.Term < r.Term {
				// 如果对方的term要小于自己的，那么直接返回
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgAppendResponse,
					To:      m.From,
					From:    m.To,
					Term:    r.Term,
					Reject:  true,
				})
				return ErrProposalDropped
			} else {
				r.Lead = m.From
				r.handleAppendEntries(m)
			}
		case pb.MessageType_MsgHup:
			r.becomeCandidate()
			r.startNewRoundVoteRequest()
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgAppend:
			if m.Term < r.Term {
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgAppendResponse,
					To:      m.From,
					From:    m.To,
					Term:    r.Term,
					Reject:  true,
				})
				return ErrProposalDropped
			} else {
				// 如果收到的请求比自己的大，说明有了其他的leader，转变自己的状态为follower并添加日志
				r.becomeFollower(m.Term, m.From)
				r.handleAppendEntries(m)
			}

		case pb.MessageType_MsgRequestVoteResponse:
			if m.Term > r.Term {
				r.becomeFollower(m.Term, None)
				break
			} else if m.Term < r.Term {
				break
			}

			r.votes[m.From] = !m.Reject
			count := 0
			for _, v := range r.votes {
				if v {
					count++
				}
			}
			// 比较对象是所有人
			if count >= len(r.Prs)/2+1 {
				r.becomeLeader()
				for k := range r.Prs {
					if k != r.id {
						r.sendHeartbeat(k)
					}
				}
			} else {
				// TODO: 这个地方非常奇怪
				if len(r.votes) == len(r.Prs) {
					// 如果所有人已经投票，但是却没有赢，那么就需要变为follower
					r.becomeFollower(r.Term, None)
				}
				//r.becomeFollower(r.Term, None)
			}
		case pb.MessageType_MsgHeartbeat:
			if m.Term >= r.Term {
				r.becomeFollower(m.Term, m.From)
				r.electionElapsed = -rand.Intn(r.electionTimeout) - 1
				// TODO: 暂时不回复消息
			}
		case pb.MessageType_MsgHup:
			r.Term++
			r.startNewRoundVoteRequest()
		case pb.MessageType_MsgRequestVote:
			if m.Term > r.Term {
				r.becomeFollower(m.Term, None)
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgRequestVoteResponse,
					To:      m.From,
					From:    r.id,
					Term:    r.Term,
					Reject:  !r.allowVote(m),
				})
			} else {
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgRequestVoteResponse,
					To:      m.From,
					From:    r.id,
					Term:    r.Term,
					Reject:  true,
				})
			}
		}

	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgAppend:
			if m.Term > r.Term {
				r.becomeFollower(m.Term, None)
			} else {
				//panic("leader收到小于当前term的消息MessageType_MsgAppend")
			}
		case pb.MessageType_MsgAppendResponse:
			if m.Term == r.Term {
				// 收到回复的消息
				if m.Reject {
					// 如果被拒绝，说明存在冲突，减小Next
					r.Prs[m.From].Next--
					if r.Prs[m.From].Next == r.Prs[m.From].Match {
						panic("到达Match点依然没有匹配")
					}
					// 如果有冲突就继续发送
					r.sendAppend(m.From)
				} else {
					r.Prs[m.From].Match = m.Index
					r.Prs[m.From].Next = m.Index + 1
					// 如果commitIndex增加则需要更新follower的commitIndex
					if r.updateCommit() {
						for k := range r.Prs {
							if k != r.id {
								r.sendAppend(k)
							}
						}
					}
				}
			} else if m.Term > r.Term {
				panic("leader见鬼了")
			}

		case pb.MessageType_MsgBeat:
			// 如果heartbeat超时，那么需要发送消息
			for k := range r.Prs {
				if k != r.id {
					r.sendHeartbeat(k)
				}
			}
			r.heartbeatElapsed = 0
		case pb.MessageType_MsgPropose:
			// 从client来的请求没有term/index等信息，需要自己添加
			for _, ent := range m.Entries {
				r.RaftLog.AppendEntry(&pb.Entry{
					EntryType: ent.EntryType,
					Term:      r.Term,
					Index:     r.RaftLog.LastIndex() + 1,
					Data:      ent.Data,
				})
			}
			for k := range r.Prs {
				if k != r.id {
					r.sendAppend(k)
				}
			}
			r.Prs[r.id].Match = r.RaftLog.LastIndex()
			r.Prs[r.id].Next = r.Prs[r.id].Match + 1
			if len(r.Prs) == 1 {
				// 只有leader一个服务器，那么直接更新commit
				r.RaftLog.committed = r.RaftLog.LastIndex()
			}
		case pb.MessageType_MsgRequestVote:
			if m.Term > r.Term {
				r.becomeFollower(m.Term, None)
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgRequestVoteResponse,
					To:      m.From,
					From:    r.id,
					Term:    r.Term,
					Reject:  !r.allowVote(m),
				})
			} else {
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgRequestVoteResponse,
					To:      m.From,
					From:    r.id,
					Term:    r.Term,
					Reject:  true,
				})
			}

		}
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	// 更新term
	r.Term = max(r.Term, m.Term)

	if m.Index == 0 && r.RaftLog.LastIndex() == 0 {
		// 初始情况，全部添加
		r.RaftLog.AppendEntries(m.Entries)
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Index:   r.RaftLog.LastIndex(),
			Reject:  false,
		})
		if m.Commit > r.RaftLog.committed {
			r.RaftLog.committed = min(r.RaftLog.LastIndex(), m.Commit)
		}
		return
	}

	// 至少要有一个位置是相邻接的
	if r.RaftLog.LastIndex() >= m.Index {
		if r.RaftLog.GetEntry(m.Index).Term != m.LogTerm {
			// 如果日志在 prevLogIndex 位置处的日志条目的任期号和 prevLogTerm 不匹配，则返回 false （5.3 节），甚至不修改commitIndex
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgAppendResponse,
				To:      m.From,
				From:    r.id,
				Term:    r.Term,
				Reject:  true,
			})
			return
		} else {
			// 遍历之后的entries，并添加到自己的记录中
			// 具体先截断到匹配的位置之前，然后从匹配位置全部添加
			for _, ent := range m.Entries {
				term, _ := r.RaftLog.Term(ent.Index)
				if term != ent.Term {
					if term != 0 {
						// 如果冲突是真的"冲突"，需要更新自己的stableIndex
						r.RaftLog.stabled = min(ent.Index-1, r.RaftLog.stabled)
					}
					r.RaftLog.entries = r.RaftLog.entries[0:m.Index]
					r.RaftLog.AppendEntries(m.Entries)
					break
				}
			}
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgAppendResponse,
				To:      m.From,
				From:    r.id,
				Term:    r.Term,
				Reject:  false,
				Index:   r.RaftLog.LastIndex(),
				Commit:  r.RaftLog.committed,
			})
		}
	} else {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
		})
		return
	}
	// 不能使用r.Raft.LastIndex，commit的更改必须来自于leader
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Index + uint64(len(m.Entries)), m.Commit)
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
