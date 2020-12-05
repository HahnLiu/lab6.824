package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Term    int // 版本号
	Index   int
	Command interface{}
}

type ServerRole string

// 服务器状态
const (
	FOLLOWER  ServerRole = "follower"
	CANDIDATE ServerRole = "candidate"
	LEADER    ServerRole = "leader"
	DEAD      ServerRole = "dead"
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 所有服务器的持久化状态

	currentTerm int        // 服务器最大版本号 // start by 1
	voteFor     int        // 当前版本号的投票
	log         []LogEntry // 每一日志保存了操作以及被leader接收时的term(lab2-A只实现term) // index从1开始记

	// 易失状态

	// 所有服务器

	commitIndex int // leader已确认的大部分server都已收到的log index（记录到rf.log并持久化）
	lastApplied int // server提交给raft上层应用的最后index

	// leader

	nextIndex  []int // 应该发送给follower的下一个log的index，由matchIndex更新
	matchIndex []int // 每个follower的log同步进度（初始化为0）

	lastBroadcastTime time.Time // leader上次发送心跳包的时间
	applyCh           chan ApplyMsg

	// 用于超时选举

	role           ServerRole
	lastActiveTime time.Time // 收到心跳包；给candidate投票；请求其他节点投票
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.role == LEADER
	return term, isleader
}

// save Raft's persistent state to stable storage,
//
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// 持久化
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
// 当server从crash中恢复，读取持久化的term voteFor log
//
func (rf *Raft) readPersist(data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var voteFor int
	var log []LogEntry
	d.Decode(&term)
	d.Decode(&voteFor)
	d.Decode(&log)
	rf.currentTerm = term
	rf.voteFor = voteFor
	rf.log = log
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	// NOTE: 用于candidate向所有server发送vote请求

	Term         int // candidate任期
	CandidateId  int // candidate Id号
	LastLogIndex int // candidate的最后一个log index
	LastLogTerm  int // candidate最后一个log的任期
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // server当前任期
	VoteGranted bool // 该candidate是否获得投票
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int // 用于判断follower与leader之间
	PrevLogTerm  int // 不相同的log，以同步所有log
	Entries      []LogEntry
	LeaderCommit int // 用于告知follower大部分server已经持久化的log，这部分log（lastApplied到commitIndex）可以提交给上层应用
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int // 用于给leader判断给从哪里
	ConflictTerm  int // 重新发送log以同步所有log
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	rf.lastActiveTime = time.Now()

	DPrintf("RequestVote candidate[%d]: term[%d] lastlogindex[%d] lastlogterm[%d], server[%d]: term[%d] lastlogindex[%d] lastlogterm[%d]",
		args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm,
		rf.me, rf.currentTerm, rf.getLastLogIndex(), rf.getLastLogTerm())
	if args.Term < rf.currentTerm {
		// 有更新的任期，candidate不该成为leader
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		// candidate任期更新，有资格成为leader
		rf.convertToFollower(args.Term)
	}

	if (rf.voteFor == -1 || rf.voteFor == args.CandidateId) && rf.isLogMoreUptodate(args.LastLogIndex, args.LastLogTerm) {
		rf.voteFor = args.CandidateId
		reply.VoteGranted = true
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	DPrintf("AppendEntires server[%d]:term[%d] prevLogIndex[%d] prevLogTerm[%d] leaderCommit[%d] ---> server[%d]:term[%d] lastLogIndex[%d] lastLogTerm[%d] commitIndex[%d]",
		args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, rf.me, rf.currentTerm, rf.getLastLogIndex(), rf.getLastLogTerm(), rf.commitIndex)

	// 用conflictIndex记录第一个不同步的log
	// 目前对于不匹配的log，会不断回滚
	isSuccess := false
	conflictIndex := 0
	conflictTerm := 0
	rf.lastActiveTime = time.Now()

	if args.Term < rf.currentTerm {
		// 收到旧leader的消息
		rf.handleReply(reply, isSuccess, conflictIndex, conflictTerm)
		return
	}

	if args.Term > rf.currentTerm {
		// 收到leader的请求且发现自己没有更新任期
		rf.convertToFollower(args.Term)
	}

	if len(rf.log)-1 < args.PrevLogIndex {
		// server的len(rf.log)-1小于leader记录的最后一条索引
		// 说明server丢失了len(rf.log)到leader.nextIndex[server]之间的log
		conflictIndex = len(rf.log)
		conflictTerm = 0
		rf.handleReply(reply, isSuccess, conflictIndex, conflictTerm)
		return
	}

	if args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// leader记录的follower最后一条log的任期与follower上的log不同步
		// follower应该告知leader第一个不同步的term的位置
		// 如果leader找不到这个不同步的任期，则从这个位置开始重新同步
		// 如果leader能找到这个任期的log，则从leader的log上最后一条这个任期的log往后同步
		conflictTerm = rf.log[args.PrevLogIndex].Term
		for index := 1; index < len(rf.log); index++ {
			// 找到第一个冲突term的位置
			if rf.log[index].Term == conflictTerm {
				conflictIndex = index
				break
			}
		}
		rf.handleReply(reply, isSuccess, conflictIndex, conflictTerm)
		return
	}

	// leader发送的prevLogIndex之前的log与follower同步了
	// follower可以接收leader的log，把其添加到log中并持久化
	for i, logEntry := range args.Entries {
		index := args.PrevLogIndex + 1 + i
		if index > len(rf.log)-1 {
			rf.log = append(rf.log, logEntry)
		} else {
			if rf.log[index].Term != logEntry.Term {
				// 删除follower与leader不同步的log
				rf.log = rf.log[:index]
				rf.log = append(rf.log, logEntry)
			} // term一样什么也不做，跳过
		}
	}
	rf.persist()

	// leader告知follower可以是否可以向上层提交log
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		if len(rf.log)-1 < rf.commitIndex {
			rf.commitIndex = len(rf.log) - 1
		}
	}

	isSuccess = true
	rf.handleReply(reply, isSuccess, conflictIndex, conflictTerm)
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	term = rf.currentTerm
	isLeader = rf.role == LEADER
	if isLeader {
		index = len(rf.log)
		entry := LogEntry{
			Term:    term,
			Index:   index,
			Command: command,
		}
		rf.log = append(rf.log, entry)
		rf.persist()
	}

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
// 初始化raft
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	rf.currentTerm = 1
	rf.voteFor = -1
	//如果slice的第一个元素为nil会导致gob Encode/Decode为空,这里改为一个空的LogEntry便于编码。
	rf.log = make([]LogEntry, 0)
	emptylog := LogEntry{}
	rf.log = append(rf.log, emptylog)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh

	rf.role = FOLLOWER
	rf.lastActiveTime = time.Now()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.onElection()
	go rf.StateMonitor()

	return rf
}

// 选举
func (rf *Raft) onElection() {
	for {
		startTime := time.Now()
		timeout := 200 + rand.Intn(100)
		time.Sleep(time.Duration(timeout) * time.Millisecond)

		rf.mu.Lock()

		if rf.role == DEAD {
			rf.mu.Unlock()
			return
		}
		if rf.lastActiveTime.Before(startTime) {
			if rf.role != LEADER {
				go rf.kickoffElection()
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) StateMonitor() {
	for {
		time.Sleep(10 * time.Millisecond)

		rf.mu.Lock()
		if rf.killed() {
			rf.role = DEAD
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) kickoffElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.lastActiveTime = time.Now()
	rf.convertToCandidate()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}
	numOfVote := 1
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(server, &args, &reply)
			if !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if !reply.VoteGranted {
				if reply.Term > rf.currentTerm {
					// 有任期更新的server，candidate转为follower
					rf.convertToFollower(reply.Term)
				}
				return
			}
			numOfVote++
			if numOfVote > len(rf.peers)/2 {
				rf.convertToLeader()
				go rf.sendHeartBeat()
				return
			}
		}(i)
	}
}

func (rf *Raft) convertToLeader() {
	defer rf.persist()
	if rf.role != CANDIDATE {
		return
	}
	DPrintf("server[%d] %v->leader, term[%d]\n", rf.me, rf.role, rf.currentTerm)
	rf.role = LEADER
	rf.nextIndex = make([]int, len(rf.peers))
	for i, _ := range rf.peers {
		// 每个server下一个应该接受的log index初始化为leader的最后log index + 1
		rf.nextIndex[i] = rf.getLastLogIndex() + 1 // 默认lastLogIndex之前的日志都同步好了
		rf.matchIndex = make([]int, len(rf.peers))
	}
}

func (rf *Raft) convertToCandidate() {
	defer rf.persist()
	DPrintf("server[%d] %v->candidate, term[%d]\n", rf.me, rf.role, rf.currentTerm)
	rf.role = CANDIDATE
	rf.currentTerm++
	rf.voteFor = rf.me
}

func (rf *Raft) convertToFollower(term int) {
	defer rf.persist()
	DPrintf("server[%d] %v->follower, term[%d]\n", rf.me, rf.role, rf.currentTerm)
	rf.role = FOLLOWER
	rf.currentTerm = term
	rf.voteFor = -1
}

func (rf *Raft) sendHeartBeat() {
	for !rf.killed() {
		time.Sleep(time.Duration(100) * time.Millisecond)

		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.role != LEADER {
				return
			}

			now := time.Now()
			if now.Sub(rf.lastBroadcastTime) < 100*time.Millisecond {
				return // NOTE:
			}
			rf.lastBroadcastTime = now

			for i, _ := range rf.peers {
				if i == rf.me {
					continue
				}

				nextIndex := rf.nextIndex[i]
				entries := make([]LogEntry, 0)
				entries = append(entries, rf.log[nextIndex:]...)

				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.getPrevLogIndex(i),
					PrevLogTerm:  rf.getPrevLogTerm(i),
					Entries:      entries,
					LeaderCommit: rf.commitIndex,
				}

				go func(server int, args *AppendEntriesArgs) {
					reply := AppendEntriesReply{}
					ok := rf.sendAppendEntries(server, args, &reply)
					if !ok {
						return
					}
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if args.Term != rf.currentTerm {
						return // NOTE:任期不同，有别的server成为leader并更改了旧leader的任期
					}
					if reply.Term > rf.currentTerm {
						// 有任期更新的server
						rf.convertToFollower(reply.Term)
						return
					}
					if reply.Success == true {
						DPrintf("server[%d->%d] send HeartBeat success, leadercommit[%d], matchIndex[%d->%d]",
							rf.me, server, args.LeaderCommit, args.PrevLogIndex, args.PrevLogIndex+len(args.Entries))
						rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
						rf.nextIndex[server] = rf.matchIndex[server] + 1
						// 看看是否大部分server都更新的log index，如果有更新，说明可以往上层应用提交
						rf.advanceCommitIndex()
					} else {
						if reply.ConflictTerm != 0 {
							// server的PrevLog的term冲突
							// 找到leader的conflictTerm出现的最后位置
							// 如果没有再找conflictIndex
							lastIndexOfconflictTerm := 0
							for index := args.PrevLogIndex; index > 0; index-- {
								if rf.log[index].Term == reply.ConflictTerm {
									lastIndexOfconflictTerm = index
									break
								}
							}
							if lastIndexOfconflictTerm == 0 {
								// leader的log中没有reply.ConflictTerm这个任期的日志
								// 那么从follower首次出现ConflictTerm的位置开始同步
								rf.nextIndex[server] = reply.ConflictIndex
							} else {
								// 从leader的log中最后一个reply.ConflictTerm的下一个log开始同步
								rf.nextIndex[server] = lastIndexOfconflictTerm + 1
							}
						} else {
							// follower记录的日志缺失
							rf.nextIndex[server] = reply.ConflictIndex
						}
					}
				}(i, &args)
			}
		}()

	}
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) getLastLogTerm() int {
	lastLogIndex := rf.getLastLogIndex()
	if lastLogIndex == 0 {
		return 0
	} else {
		return rf.log[lastLogIndex].Term
	}
}

func (rf *Raft) getPrevLogIndex(server int) int {
	return rf.nextIndex[server] - 1
}

func (rf *Raft) getPrevLogTerm(server int) int {
	prevLogIndex := rf.getPrevLogIndex(server)
	if prevLogIndex == 0 {
		return 0
	} else {
		return rf.log[prevLogIndex].Term
	}
}

func (rf *Raft) advanceCommitIndex() {
	// matchIndex记录着所有server已经获得的log
	// 取其中位数（多数server）的值作为raft集群可以向上层提交的log
	sortedMatchIndex := make([]int, len(rf.matchIndex))
	copy(sortedMatchIndex, rf.matchIndex)
	sortedMatchIndex[rf.me] = len(rf.log) - 1
	sort.Ints(sortedMatchIndex)
	N := sortedMatchIndex[len(rf.peers)/2]
	DPrintf("server[%d] advanceCommitIndex, N[%d], rf.commitIndex[%d]， log[N].Term[%d], rf.Term[%d]",
		rf.me, N, rf.commitIndex, rf.log[N].Term, rf.currentTerm)
	if rf.role == LEADER && N > rf.commitIndex && rf.log[N].Term == rf.currentTerm {
		rf.commitIndex = N // 更新commit然后apply
		rf.applyLog()
	}
}

func (rf *Raft) handleReply(reply *AppendEntriesReply, isSuccess bool, conflictIndex int, conflictTerm int) {
	rf.applyLog()
	reply.Success = isSuccess
	reply.Term = rf.currentTerm
	reply.ConflictIndex = conflictIndex
	reply.ConflictTerm = conflictTerm
}

// 向上层提交log
func (rf *Raft) applyLog() {
	DPrintf("server[%d] in term[%d] in ApplyLog, rf.commitIndex[%d], rf.lastApplied[%d]",
		rf.me, rf.currentTerm, rf.commitIndex, rf.lastApplied)
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied += 1
		entry := rf.log[rf.lastApplied]
		msg := ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: entry.Index,
		}
		rf.applyCh <- msg
	}
}

// index : candidate lastLogIndex
// term : candidate lastLogTerm
func (rf *Raft) isLogMoreUptodate(index int, term int) bool {
	return term > rf.getLastLogTerm() || (term == rf.getLastLogTerm() && index >= rf.getLastLogIndex())
}
