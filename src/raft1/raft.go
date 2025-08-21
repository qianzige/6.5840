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
	last_commit        int                   // 最新的未提交的日志索引
	applyCh            chan raftapi.ApplyMsg // 用于发送已提交的日志条目
	nextIndex          []int                 // 每一个follower节点的同步起始位置索引
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
	Term         int // 候选人的任期
	CandidateId  int // 候选人的ID
	LastLogIndex int // 候选人最后一条日志的索引
	LastLogTerm  int // 候选人最后一条日志的任期
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
		if rf.is_leader {
			rf.is_leader = false // 退出leader状态
			fmt.Printf("server %d quit leader due to higher term in RequestVote\n", rf.me)
		}
	} else {
		reply.Term = rf.term
		reply.Voted = false
		return
	}

	// 检查候选人的日志是否至少与自己一样新
	lastLogIndex := len(rf.log) - 1
	var lastLogTerm int
	if lastLogIndex >= 0 {
		lastLogTerm = rf.log[lastLogIndex].Term
	}

	// 如果自己的日志更新，拒绝投票
	if lastLogTerm > args.LastLogTerm ||
		(lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex) {
		reply.Term = rf.term
		reply.Voted = false
		return
	}

	// 投票给候选人
	rf.voted = args.Term
	rf.term = args.Term
	reply.Voted = true

	// 重置选举超时
	rf.last_ack = time.Now()

	fmt.Printf("server %d votes to server %d in term %d\n", rf.me, args.CandidateId, args.Term)
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
var startMutex sync.Mutex

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	startMutex.Lock()
	defer startMutex.Unlock()
	rf.mu.Lock()

	// 快速检查是否是leader，如果不是则立即返回
	if !rf.is_leader {
		defer rf.mu.Unlock()
		return rf.last_commit, rf.term, false
	}

	// 添加日志条目
	logEntry := LogEntry{Command: command, Term: rf.term}
	rf.log = append(rf.log, logEntry)
	index := len(rf.log)
	term := rf.term

	fmt.Printf("server %d is starting with cmd: %s.\n", rf.me, command)

	// 在释放锁前准备好所有需要的数据
	peers := make([]*labrpc.ClientEnd, len(rf.peers))
	copy(peers, rf.peers)
	nextIndices := make([]int, len(rf.nextIndex))
	copy(nextIndices, rf.nextIndex)

	rf.mu.Unlock() // 尽早释放锁

	// 启动一个新的goroutine来处理复制和重试
	go func() {
		// 最大重试次数
		maxRetries := 3

		for retry := 0; retry < maxRetries; retry++ {
			// 创建计数器
			success := make([]int, 0) // 成功复制的节点列表
			var mu sync.Mutex
			var wg sync.WaitGroup

			// 同步日志（不持有锁）
			for i := range peers {
				if i != rf.me {
					wg.Add(1)
					go func(i int, nextIndex int) {
						defer wg.Done()

						// 在发送RPC前获取锁以读取最新状态
						rf.mu.Lock()
						if !rf.is_leader || rf.term != term {
							rf.mu.Unlock()
							return
						}

						// 准备RPC参数
						prevLogIndex := nextIndex - 1
						var prevLogTerm int
						if prevLogIndex >= 0 && prevLogIndex < len(rf.log) {
							prevLogTerm = rf.log[prevLogIndex].Term
						}
						entries := make([]LogEntry, len(rf.log[nextIndex:len(rf.log)]))
						copy(entries, rf.log[nextIndex:len(rf.log)])
						leaderCommit := rf.last_commit

						rf.mu.Unlock() // 发送RPC前释放锁

						args := AppendEntriesArgs{
							Term:         term,
							LeaderId:     rf.me,
							Entries:      entries,
							LeaderCommit: leaderCommit,
							PrevLogIndex: prevLogIndex,
							PrevLogTerm:  prevLogTerm,
							Type:         "replicate",
						}
						reply := AppendEntriesReply{Term: term, Success: false}

						// 创建一个通道用于接收RPC调用结果
						done := make(chan bool, 1)

						// 在一个新的goroutine中执行RPC调用
						go func() {
							ok := rf.peers[i].Call("Raft.AppendEntries", &args, &reply)
							done <- ok
						}()

						// 等待RPC调用完成或超时
						select {
						case ok := <-done:
							// RPC调用完成
							if ok {
								rf.mu.Lock()
								if reply.Success {
									mu.Lock()
									success = append(success, i)
									mu.Unlock()

									// 更新nextIndex
									rf.nextIndex[i] = args.PrevLogIndex + 1 + len(args.Entries)
								} else if reply.Term > rf.term {
									// 发现更高任期，更新自己的状态
									rf.term = reply.Term
									rf.is_leader = false
									fmt.Printf("server %d quit leader at term %d\n", rf.me, rf.term)
								} else {
									// 日志不一致，减少nextIndex
									rf.nextIndex[i] = reply.PrevLogIndex + 1
								}
								rf.mu.Unlock()
							}
						case <-time.After(1 * time.Second):
							// 超时，不做任何处理，直接返回
							fmt.Printf("server %d send replicate to server %d timed out after 1s\n", rf.me, i)
						}
					}(i, nextIndices[i])
				}
			}

			// 等待所有RPC完成
			wg.Wait()

			// 检查是否有过半节点复制成功
			if len(success)+1 > len(peers)/2 {
				rf.mu.Lock()
				// 确保我们仍然是leader，并且term没有变化
				if rf.is_leader && rf.term == term {
					// 更新last_commit
					oldLastCommit := rf.last_commit
					rf.last_commit = len(rf.log)

					// 获取需要应用的日志条目
					entriesToApply := make([]LogEntry, len(rf.log[oldLastCommit:rf.last_commit]))
					copy(entriesToApply, rf.log[oldLastCommit:rf.last_commit])

					rf.mu.Unlock()

					// 应用日志（不持有锁）
					if len(entriesToApply) > 0 {
						rf.applyLogEntries(oldLastCommit, index, entriesToApply)
					}

					// 通知其他节点更新LeaderCommit
					for _, i := range success {
						if i != rf.me {
							go func(i int) {
								rf.mu.Lock()
								if !rf.is_leader || rf.term != term {
									rf.mu.Unlock()
									return
								}

								// 准备RPC参数
								prevLogIndex := rf.nextIndex[i] - 1
								var prevLogTerm int
								if prevLogIndex >= 0 && prevLogIndex < len(rf.log) {
									prevLogTerm = rf.log[prevLogIndex].Term
								}

								rf.mu.Unlock()

								args := AppendEntriesArgs{
									Term:         term,
									LeaderId:     rf.me,
									Entries:      nil,
									LeaderCommit: index,
									PrevLogIndex: prevLogIndex,
									PrevLogTerm:  prevLogTerm,
									Type:         "apply",
								}
								reply := AppendEntriesReply{}
								rf.peers[i].Call("Raft.AppendEntries", &args, &reply)
							}(i)
						}
					}

					// 复制成功，不需要继续重试
					return
				} else {
					rf.mu.Unlock()
					// 不再是leader，停止重试
					return
				}
			}

			// 如果不是最后一次重试，等待一段时间后再次尝试
			if retry < maxRetries-1 {
				// 检查是否仍然是leader
				rf.mu.Lock()
				stillLeader := rf.is_leader && rf.term == term
				rf.mu.Unlock()

				if !stillLeader {
					// 不再是leader，停止重试
					return
				}

				// 等待一段时间后重试
				time.Sleep(time.Duration(100*(retry+1)) * time.Millisecond)

				// 更新nextIndices以便下次重试
				rf.mu.Lock()
				nextIndices = make([]int, len(rf.nextIndex))
				copy(nextIndices, rf.nextIndex)
				rf.mu.Unlock()

				fmt.Printf("server %d retry %d for cmd at index %d\n", rf.me, retry+1, index)
			}
		}
	}()

	return index, term, true
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
		// 检查是否需要发起选举
		rf.mu.Lock()
		if !rf.is_leader && rf.last_ack.Add(time.Duration(2*rf.heartbeat_duration)*time.Millisecond).Before(time.Now()) {

			rf.term++
			rf.voted = rf.term
			currentTerm := rf.term
			rf.mu.Unlock()

			// 选举过程（不持有锁）
			rf.startElection(currentTerm)
		} else {
			rf.mu.Unlock()
		}

		// 随机休眠
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) startElection(term int) {
	fmt.Printf("server %d is electing for term %d\n", rf.me, term)

	votes := 1 // 自己的一票
	var mu sync.Mutex
	var wg sync.WaitGroup

	// 设置超时
	electionDone := make(chan struct{})
	timeout := make(chan struct{})

	go func() {
		time.Sleep(time.Duration(rf.heartbeat_duration) * time.Millisecond)
		close(timeout)
	}()

	// 向其他节点请求投票
	for i := range rf.peers {
		if i != rf.me {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()

				lastLogIndex := len(rf.log) - 1
				lastLogTerm := 0
				if lastLogIndex >= 0 {
					lastLogTerm = rf.log[lastLogIndex].Term
				}

				args := RequestVoteArgs{Term: term, CandidateId: rf.me,
					LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}
				reply := RequestVoteReply{}

				if rf.sendRequestVote(i, &args, &reply) {
					rf.mu.Lock()
					if reply.Voted {
						mu.Lock()
						votes++
						mu.Unlock()
					} else if reply.Term > term {
						rf.term = reply.Term
					}
					rf.mu.Unlock()
				}
			}(i)
		}
	}

	// 等待所有投票结果或超时
	go func() {
		wg.Wait()
		close(electionDone)
	}()

	// 等待选举完成或超时
	select {
	case <-electionDone:
	case <-timeout:
	}

	// 检查是否获得多数票
	rf.mu.Lock()
	if rf.term == term && votes > len(rf.peers)/2 {
		fmt.Printf("server %d become a leader for term %d\n", rf.me, term)
		rf.is_leader = true

		// 初始化nextIndex
		for i := range rf.nextIndex {
			rf.nextIndex[i] = len(rf.log)
		}

		// 发送心跳
		rf.sendHeartbeat(&AppendEntriesArgs{Term: term, LeaderId: rf.me, Type: "heartbeats"}, &AppendEntriesReply{})
	}
	rf.mu.Unlock()
}

type AppendEntriesArgs struct {
	LeaderId     int
	Term         int
	Entries      []LogEntry
	LeaderCommit int
	PrevLogIndex int
	PrevLogTerm  int
	Type         string
}

type AppendEntriesReply struct {
	Success      bool // 表示追加日志是否成功
	Term         int  // 当前任期号，用于领导者更新自己
	PrevLogIndex int  // follower节点希望的日志位置
}

// AppendEntries handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 记录日志（可以考虑移到锁外）
	if args.Type == "heartbeats" {
		fmt.Printf("server %d receive heartbeats from server %d, args_term: %d, my_term: %d\n", rf.me, args.LeaderId, args.Term, rf.term)
	} else if args.Type == "replicate" {
		fmt.Printf("server %d receive append entries from server %d, args_term: %d, my_term: %d\n", rf.me, args.LeaderId, args.Term, rf.term)
	} else {
		fmt.Printf("server %d is applying append entries from server %d, args_term: %d, my_term: %d\n", rf.me, args.LeaderId, args.Term, rf.term)
	}

	// 检查任期
	if args.Term < rf.term {
		reply.Term = rf.term
		reply.Success = false
		return
	}

	// 更新任期和状态
	if args.Term > rf.term {
		rf.term = args.Term
		rf.voted = args.Term
		if rf.is_leader {
			rf.is_leader = false
			fmt.Printf("server %d quit leader\n", rf.me)
		}
	}

	// 重置选举超时
	rf.last_ack = time.Now()

	// 心跳包直接返回
	if args.Type == "heartbeats" {
		return
	}

	// 检查日志一致性
	if args.PrevLogIndex >= len(rf.log) ||
		(args.PrevLogIndex >= 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		reply.Success = false
		reply.Term = rf.term
		reply.PrevLogIndex = rf.last_commit - 1
		return
	}

	// 处理日志条目
	reply.Success = true
	reply.Term = rf.term

	if args.Type == "replicate" {
		// 追加新的日志条目
		if len(args.Entries) > 0 {
			// 找到第一个需要追加的条目在args.Entries中的索引
			appendStartIdx := 0

			// 如果follower的日志长度超过PrevLogIndex+1，说明有一些条目可能已经存在
			// 需要检查这些条目是否与leader发送的条目一致
			for i := 0; i < len(args.Entries) && args.PrevLogIndex+1+i < len(rf.log); i++ {
				if rf.log[args.PrevLogIndex+1+i].Term != args.Entries[i].Term {
					// 发现冲突，删除从这个点开始的所有日志
					rf.log = rf.log[:args.PrevLogIndex+1+i]
					break
				}
				appendStartIdx = i + 1
			}

			// 追加新的日志条目
			if appendStartIdx < len(args.Entries) {
				rf.log = append(rf.log, args.Entries[appendStartIdx:]...)
			}
		}
	}

	if args.Type == "apply" {
		// 更新提交索引
		if args.LeaderCommit > rf.last_commit {
			newCommitIndex := min(args.LeaderCommit, len(rf.log))

			// 如果有新的提交，应用日志
			if newCommitIndex > rf.last_commit {
				oldCommit := rf.last_commit
				entriesToApply := make([]LogEntry, len(rf.log[oldCommit:newCommitIndex]))
				copy(entriesToApply, rf.log[oldCommit:newCommitIndex])

				// 更新提交索引
				rf.last_commit = newCommitIndex

				// 在锁外应用日志
				go rf.applyLogEntries(oldCommit, newCommitIndex, entriesToApply)
			}
		}
	}
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
		rf.mu.Lock()
		isLeader := rf.is_leader
		term := rf.term
		rf.mu.Unlock()

		if isLeader {
			// 发送心跳（不持有锁）
			args := &AppendEntriesArgs{
				Term:     term,
				LeaderId: rf.me,
				Type:     "heartbeats",
			}
			reply := &AppendEntriesReply{}

			for i := range rf.peers {
				if i != rf.me {
					go func(i int) {
						rf.peers[i].Call("Raft.AppendEntries", args, reply)
					}(i)
				}
			}
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

	// 初始化状态
	rf.voted = 0
	rf.last_ack = time.Now()
	rf.is_leader = false
	rf.term = 0
	rf.heartbeat_duration = 200
	rf.nextIndex = make([]int, len(rf.peers))
	rf.log = make([]LogEntry, 0)
	rf.applyCh = applyCh

	// 从持久化状态恢复
	rf.readPersist(persister.ReadRaftState())

	// 启动后台协程
	go rf.ticker()
	go rf.heartbeatTicker()

	return rf
}

// applyLogEntries 方法用于将日志条目应用到状态机，不持有锁
func (rf *Raft) applyLogEntries(startIndex, endIndex int, entries []LogEntry) {
	for i, entry := range entries {
		// 计算当前日志条目的索引
		index := startIndex + i

		// 创建 ApplyMsg
		msg := raftapi.ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: index + 1,
		}

		// 发送到 applyCh
		rf.applyCh <- msg

		fmt.Printf("server %d applied log entry at index %d\n", rf.me, index)
	}

	// 更新lastApplied（需要加锁）
	rf.mu.Lock()
	rf.last_commit = endIndex
	rf.mu.Unlock()
}
