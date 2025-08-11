package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"fmt"
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	term               int       // term of this raft
	is_leader          bool      // whether this peer is leader
	voted              int       // whether this peer has voted
	last_ack           time.Time // last ack time
	heartbeat_duration int       // ms
	log                []LogEntry
	last_commit        int
	// 新增字段
	applyCh chan raftapi.ApplyMsg // 用于发送已提交的日志条目
}
type LogEntry struct {
	Command interface{}
	Term    int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	term = rf.term
	isleader = rf.is_leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term        int // term of this peer
	CandidateId int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term  int
	Voted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.voted {
		fmt.Printf("server %d votes to server %d in term %d\n", rf.me, args.CandidateId, args.Term)
		rf.voted = args.Term
		rf.term = args.Term
		reply.Voted = true
	} else {
		reply.Term = rf.term
	}
	return
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	logEntry := LogEntry{
		Command: command,
		Term:    rf.term,
	}

	// 将命令追加到日志中
	rf.log = append(rf.log, logEntry)

	// 获取新日志条目的索引
	index := len(rf.log)
	term := rf.term
	isLeader := rf.is_leader

	// Your code here (3B).
	fmt.Printf("server %d is starting.\n", rf.me)
	if !rf.is_leader {
		return rf.last_commit, rf.term, false
	} else {
		// 创建一个计数器，用于跟踪成功复制的节点数量
		successCount := 1 // 包括自己
		var mu sync.Mutex
		var wg sync.WaitGroup

		// 同步日志
		for i := range rf.peers {
			if i != rf.me {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					args := AppendEntriesArgs{
						Term:         rf.term,
						LeaderId:     rf.me,
						Entries:      rf.log[rf.last_commit+1 : index], // 新日志条目
						LeaderCommit: rf.last_commit,                   // Leader 已经提交的日志索引
						PrevLogIndex: index,
						PrevLogTerm:  term,
					}
					reply := &AppendEntriesReply{Term: rf.term, Success: false}
					if rf.peers[i].Call("Raft.AppendEntries", args, reply) {
						if reply.Success {
							mu.Lock()
							successCount++
							mu.Unlock()
						}
					}
				}(i)
			}
		}

		// 启动一个新的goroutine来等待复制结果并处理提交
		go func() {
			wg.Wait()

			// 检查是否有过半节点复制成功
			if successCount > len(rf.peers)/2 {
				rf.mu.Lock()
				// 确保我们仍然是leader，并且term没有变化
				if rf.is_leader && rf.term == term {
					// 更新last_commit
					oldLastCommit := rf.last_commit

					// 获取需要应用的日志条目
					entriesToApply := rf.log[oldLastCommit:index]

					// 解锁以避免死锁
					rf.mu.Unlock()

					// 应用日志
					if len(entriesToApply) > 0 {
						rf.applyLog(entriesToApply)
					}

					// 应用日志
					for i := range rf.peers {
						if i != rf.me {
							go func(i int) {
								args := AppendEntriesArgs{
									Term:         rf.term,
									LeaderId:     rf.me,
									Entries:      rf.log[rf.last_commit+1 : index], // 新日志条目
									LeaderCommit: index,                            // Leader 已经提交的日志索引
								}
								reply := &AppendEntriesReply{Term: rf.term, Success: false}
								rf.peers[i].Call("Raft.AppendEntries", args, reply)
							}(i)
						}
					}
				} else {
					rf.mu.Unlock()
				}
			}
		}()
	}

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		// 检查是否需要发起选举：超时且不是leader
		needElection := rf.last_ack.Add(time.Duration(2*rf.heartbeat_duration)*time.Millisecond).Before(time.Now()) && !rf.is_leader
		rf.mu.Unlock()
		if needElection {
			fmt.Printf("server %d is electing\n", rf.me)
			rf.mu.Lock()
			rf.term = rf.term + 1
			rf.voted = rf.term
			rf.mu.Unlock()
			votes := 1
			var wg sync.WaitGroup
			var mu sync.Mutex
			var once sync.Once
			timeout := make(chan struct{})
			go func() {
				time.Sleep(time.Duration(3*rf.heartbeat_duration) * time.Millisecond)
				once.Do(func() { close(timeout) })
			}()
			// 创建一个用于同步的WaitGroup，确保所有goroutine完成
			for i := range rf.peers {
				if i != rf.me {
					wg.Add(1) // 为每个goroutine增加一个计数

					go func(i int) {
						defer wg.Done() // 完成后减去一个计数

						args := RequestVoteArgs{Term: rf.term, CandidateId: rf.me}
						reply := RequestVoteReply{Voted: false}
						ok := rf.sendRequestVote(i, &args, &reply)
						if ok {
							if reply.Voted {
								fmt.Printf("server %d got a vote from server %d\n", rf.me, i)
								// 更新投票计数
								mu.Lock()
								votes++
								mu.Unlock()
							} else {
								fmt.Printf("server %d didn't get a vote from server %d, and change term from %d to %d\n", rf.me, i, rf.term, reply.Term)
								rf.mu.Lock()
								rf.term = reply.Term
								rf.mu.Unlock()
							}
						} else {
							fmt.Printf("server %d didn't hear from server %d\n", rf.me, i)
						}
					}(i) // 将i传递给goroutine
				}
			}
			// 等待所有goroutine完成
			go func() {
				wg.Wait()
				once.Do(func() { close(timeout) })
			}()
			<-timeout
			fmt.Printf("server %d got %d votes in term %d. ", rf.me, votes, rf.term)
			if votes > len(rf.peers)/2 {
				fmt.Printf("server %d become a leader\n", rf.me)
				rf.mu.Lock()
				rf.is_leader = true
				rf.sendHeartbeat(&AppendEntriesArgs{Term: rf.term, LeaderId: rf.me}, &AppendEntriesReply{})
				rf.mu.Unlock()
			} else {
				fmt.Printf("server %d failed to become a leader\n", rf.me)
			}
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

type AppendEntriesArgs struct {
	LeaderId     int
	Term         int
	Entries      []LogEntry
	LeaderCommit int
	PrevLogIndex int
	PrevLogTerm  int
}

type AppendEntriesReply struct {
	Success bool // 表示追加日志是否成功
	Term    int  // 当前任期号，用于领导者更新自己
}

// AppendEntries handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Printf("server %d receive append entries from server %d\n", rf.me, args.LeaderId)
	if args.Term >= rf.term {
		rf.last_ack = time.Now()
		if rf.is_leader {
			rf.is_leader = false
			fmt.Printf("server %d quit leader\n", rf.me)
		}
		rf.term = args.Term

		// 检查日志一致性
		if len(rf.log) < args.PrevLogIndex ||
			(args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm) {
			reply.Success = false
			return
		}

		// 如果有冲突的日志条目，删除它们
		for i, entry := range args.Entries {
			index := args.PrevLogIndex + i
			if index < len(rf.log) {
				if rf.log[index].Term != entry.Term {
					// 删除该索引及之后的所有条目
					rf.log = rf.log[:index]
					break
				}
			} else {
				break
			}
		}

		// 追加新的日志条目
		reply.Success = true
		rf.log = append(rf.log, args.Entries...)

		// 更新提交索引
		if args.LeaderCommit > rf.last_commit {
			oldCommit := rf.last_commit
			rf.last_commit = min(args.LeaderCommit, len(rf.log))

			// 应用新提交的日志
			if rf.last_commit > oldCommit {
				entriesToApply := rf.log[oldCommit:rf.last_commit]
				rf.applyLog(entriesToApply)
			}
		}
	}
	return
}

// heartbeat sender
func (rf *Raft) sendHeartbeat(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	// 遍历所有的peers，并发执行AppendEntries
	for i := range rf.peers {
		if i != rf.me {

			go func(i int) {
				rf.peers[i].Call("Raft.AppendEntries", args, reply)
			}(i) // 将i传递给goroutine
		}
	}
}

func (rf *Raft) heartbeatTicker() {
	for rf.killed() == false {
		if rf.is_leader {
			fmt.Printf("server %d is sending heartbeat\n", rf.me)
			rf.sendHeartbeat(&AppendEntriesArgs{Term: rf.term, LeaderId: rf.me}, &AppendEntriesReply{})
		}
		time.Sleep(time.Duration(rf.heartbeat_duration) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.voted = 0
	rf.last_ack = time.Now()
	rf.is_leader = false
	rf.term = 0
	rf.heartbeat_duration = 200

	// 初始化新添加的字段
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartbeatTicker()

	return rf
}

// applyLog 方法用于将已提交的日志条目应用到状态机
// 该方法会将每个新提交的日志条目通过 applyCh 发送出去
func (rf *Raft) applyLog(entries []LogEntry) {
	fmt.Printf("server %d applying log entries: %v\n", rf.me, entries)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 遍历需要应用的日志条目
	for i, entry := range entries {
		// 计算当前日志条目的索引
		index := rf.last_commit + i + 1

		// 创建 ApplyMsg
		msg := raftapi.ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: index,
		}

		// 发送到 applyCh
		rf.applyCh <- msg

		// 更新 lastApplied
		rf.last_commit = index

		fmt.Printf("Server %d applied log entry at index %d\n", rf.me, index)
	}
}
