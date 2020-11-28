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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

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
	currentTerm int   // 服务器最大版本号
	voteFor     int   // 当前版本号的投票
	log         []int // 每一日志保存了操作以及被leader接收时的term(lab2-A只实现term)

	// 所有服务器的易失状态
	commitIndex int // 已知的最大提交索引
	lastApplied int // 当前已被应用到状态机的最大索引

	// leader的易失状态
	nextIndex  []int // 每个follower的log同步起点索引（初始化为leader log的最后一项）
	matchIndex []int // 每个follower的log同步进度（初始化为0）

	// 易失状态，用于超时选举
	role           ServerRole
	lastActiveTime time.Time // 收到心跳包；给candidate投票；请求其他节点投票
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	DPrintf("server:%d get lock\n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DPrintf("server:%d unlock\n", rf.me)
	term = rf.currentTerm
	isleader = rf.role == LEADER
	return term, isleader
}

// save Raft's persistent state to stable storage,
//
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	// NOTE: 用于candidate向所有server发送vote请求

	Term         int // candidate term
	CandidateId  int // candidate请求Id号
	LastLogIndex int // candidate最后的LogEntriy index
	LastLogTerm  int // candidate最后的LogEntriy的term号
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 当前term，用来给server更新其currentTerm字段
	VoteGranted bool // 该candidate是否获得投票
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []int
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// example RequestVote RPC handler.
// 当一个candidate发送RequestVote，所有server都会收到rpc请求
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// DPrintf("server:%d get lock\n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// defer DPrintf("server:%d unlock\n", rf.me)

	rf.lastActiveTime = time.Now()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	if rf.voteFor == -1 || rf.voteFor == args.CandidateId {
		DPrintf("server[%d] send a valid vote to candidate[%d]", rf.me, args.CandidateId)
		rf.voteFor = args.CandidateId
		reply.VoteGranted = true
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// DPrintf("server:%d get lock\n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// defer DPrintf("server:%d unlock\n", rf.me)

	rf.lastActiveTime = time.Now()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	if rf.role == CANDIDATE {
		rf.convertToFollower(args.Term)
	}
	reply.Success = true
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
	rf.log = []int{}
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.role = FOLLOWER
	rf.lastActiveTime = time.Now()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	DPrintf("server:%d init\n", rf.me)

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

		// DPrintf("onElection server:%d get lock\n", rf.me)
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
		// DPrintf("onElection server:%d unlock\n", rf.me)
	}
}

func (rf *Raft) StateMonitor() {
	for {
		time.Sleep(10 * time.Millisecond)

		// DPrintf("StateMonitor server:%d get lock\n", rf.me)
		rf.mu.Lock()
		if rf.killed() {
			rf.role = DEAD
		}
		rf.mu.Unlock()
		// DPrintf("StateMonitor server:%d unlock\n", rf.me)
	}
}

func (rf *Raft) kickoffElection() {
	// DPrintf("kickoffElection server:%d get lock\n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// defer DPrintf("kickoffElection server:%d unlock\n", rf.me)

	rf.lastActiveTime = time.Now()
	rf.convertToCandidate()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: 0,
		LastLogTerm:  0,
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
			// DPrintf("kickoffElection send msg to %d, server:%d get lock\n", server, rf.me)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			// defer DPrintf("kickoffElection send msg to %d, server:%d unlock\n", server, rf.me)
			if !reply.VoteGranted {
				if reply.Term > rf.currentTerm {
					DPrintf("server[%d] term:%d recv an invalid vote, peer[%d] term: %d\n", rf.me, rf.currentTerm, server, reply.Term)
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
	if rf.role == FOLLOWER {
		DPrintf("server[%d] get over half of votes but became follower\n, fail to become leader", rf.me)
	}
	DPrintf("server[%d] become LEADER\n", rf.me)
	rf.role = LEADER
}

func (rf *Raft) convertToCandidate() {
	defer rf.persist()
	DPrintf("server:%d become candidate\n", rf.me)
	rf.role = CANDIDATE
	rf.currentTerm++
	rf.voteFor = rf.me
}

func (rf *Raft) convertToFollower(term int) {
	defer rf.persist()
	DPrintf("server:%d become follower\n", rf.me)
	rf.role = FOLLOWER
	rf.currentTerm = term
	rf.voteFor = -1
}

func (rf *Raft) sendHeartBeat() {
	for {
		// DPrintf("sendHeartBeat check server:%d get lock\n", rf.me)
		rf.mu.Lock()
		if rf.role != LEADER || rf.killed() {
			DPrintf("server[%d] try to send heartbeat but find it not leader\n", rf.me)
			rf.mu.Unlock()
			// DPrintf("sendHeartBeat check server:%d unlock\n", rf.me)
			return
		}
		args := AppendEntriesArgs{
			Term:     rf.currentTerm,
			LeaderId: rf.me,
		}
		rf.mu.Unlock()
		// DPrintf("sendHeartBeat server:%d unlock\n", rf.me)

		for i, _ := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(server int) {
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(server, &args, &reply)
				if !ok {
					return
				}
				// DPrintf("sendHeartBeat to server:%d, server:%d get lock\n", server, rf.me)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// defer DPrintf("sendHeartBeat to server:%d, server:%d unlock\n", server, rf.me)
				if !reply.Success {
					if reply.Term > rf.currentTerm {
						rf.convertToFollower(reply.Term)
					}
				}
				return
			}(i)
		}

		time.Sleep(time.Duration(100) * time.Millisecond)
	}
}
