// Copyright 2018 The ChuBao Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package datanode

import (
	"os"
	"strconv"
	"strings"
	"time"
	"path"
	"fmt"
	"io/ioutil"
	"net"
	"encoding/json"
	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/raftstore"
	"github.com/tiglabs/containerfs/util/config"
	"github.com/tiglabs/containerfs/util/log"
	raftproto "github.com/tiglabs/raft/proto"
	"github.com/tiglabs/containerfs/storage"
	"encoding/binary"
)

type dataPartitionCfg struct {
	VolName       string              `json:"vol_name"`
	PartitionType string              `json:"partition_type"`
	PartitionId   uint32              `json:"partition_id"`
	PartitionSize int                 `json:"partition_size"`
	Peers         []proto.Peer        `json:"peers"`
	RandomWrite   bool                `json:"random_write"`
	NodeId        uint64              `json:"-"`
	RaftStore     raftstore.RaftStore `json:"-"`
}

func (dp *dataPartition) getRaftPort() (heartbeat, replicate int, err error) {
	raftConfig := dp.config.RaftStore.RaftConfig()
	heartbeatAddrParts := strings.Split(raftConfig.HeartbeatAddr, ":")
	replicateAddrParts := strings.Split(raftConfig.ReplicateAddr, ":")
	if len(heartbeatAddrParts) != 2 {
		err = errors.New("illegal heartbeat address")
		return
	}
	if len(replicateAddrParts) != 2 {
		err = errors.New("illegal replicate address")
		return
	}
	heartbeat, err = strconv.Atoi(heartbeatAddrParts[1])
	if err != nil {
		return
	}
	replicate, err = strconv.Atoi(replicateAddrParts[1])
	if err != nil {
		return
	}
	return
}

func (dp *dataPartition) StartRaft() (err error) {
	var (
		heartbeatPort int
		replicatePort int
		peers         []raftstore.PeerAddress
	)

	if heartbeatPort, replicatePort, err = dp.getRaftPort(); err != nil {
		return
	}
	for _, peer := range dp.config.Peers {
		addr := strings.Split(peer.Addr, ":")[0]
		rp := raftstore.PeerAddress{
			Peer: raftproto.Peer{
				ID: peer.ID,
			},
			Address:       addr,
			HeartbeatPort: heartbeatPort,
			ReplicatePort: replicatePort,
		}
		peers = append(peers, rp)
	}
	log.LogDebugf("start partition=%v raft peers: %s path: %s",
		dp.partitionId, peers, dp.path)
	pc := &raftstore.PartitionConfig{
		ID:      uint64(dp.partitionId),
		Applied: dp.applyId,
		Peers:   peers,
		SM:      dp,
		WalPath: dp.path,
	}

	dp.raftPartition, err = dp.config.RaftStore.CreatePartition(pc)

	return
}

func (dp *dataPartition) stopRaft() {
	if dp.raftPartition != nil {
		dp.raftPartition.Stop()
		dp.raftPartition = nil
	}
	return
}

func (dp *dataPartition) StartSchedule() {
	var isRunning bool
	truncRaftlogTimer := time.NewTimer(time.Minute * 30)
	storeAppliedTimer := time.NewTimer(time.Minute * 5)

	dumpFunc := func(applyIndex uint64) {
		log.LogDebugf("[startSchedule] partitionId=%d: applyID=%d", dp.config.PartitionId, applyIndex)
		if err := dp.storeApplyIndex(applyIndex); err != nil {
			//retry
			dp.storeC <- applyIndex
			err = errors.Errorf("[startSchedule]: dump partition=%d: %v", dp.config.PartitionId, err.Error())
			log.LogErrorf(err.Error())
		}
		isRunning = false
	}

	go func(stopC chan bool) {
		var indexes []uint64
		readyChan := make(chan struct{}, 1)
		for {
			if len(indexes) > 0 {
				if isRunning == false {
					isRunning = true
					readyChan <- struct{}{}
				}
			}
			select {
			case <-stopC:
				truncRaftlogTimer.Stop()
				storeAppliedTimer.Stop()
				return

			case <-readyChan:
				for _, idx := range indexes {
					log.LogDebugf("[startSchedule] partitionId=%d: applyID=%d", dp.config.PartitionId, idx)
					go dumpFunc(idx)
				}
				indexes = nil
			case applyId := <-dp.storeC:
				indexes = append(indexes, applyId)
				log.LogDebugf("[startSchedule] partitionId=%d: applyID=%d", dp.config.PartitionId, applyId)
			case opRaftCode := <-dp.raftC:
				if dp.raftPartition == nil && opRaftCode == opStartRaft {
					log.LogWarn("action[RaftOp] restart raft partition=%v", dp.partitionId)
					if err := dp.StartRaft(); err != nil {
						panic("start raft error")
					}
				}
			case extentId := <-dp.repairC:
				dp.ApplyErrRepair(extentId)
				dp.raftC <- opStartRaft

			case <-truncRaftlogTimer.C:
				dp.getMinAppliedId()
				if dp.minAppliedId > dp.lastTruncateId { // Has changed
					go dp.raftPartition.Truncate(dp.minAppliedId)
					dp.lastTruncateId = dp.minAppliedId
				}
			case <-storeAppliedTimer.C:
				dp.storeC <- dp.applyId
			}
		}
	}(dp.stopC)
}

func (dp *dataPartition) confAddNode(req *proto.DataPartitionOfflineRequest, index uint64) (updated bool, err error) {
	var (
		heartbeatPort int
		replicatePort int
	)
	if heartbeatPort, replicatePort, err = dp.getRaftPort(); err != nil {
		return
	}

	findAddPeer := false
	for _, peer := range dp.config.Peers {
		if peer.ID == req.AddPeer.ID {
			findAddPeer = true
			break
		}
	}
	updated = !findAddPeer
	if !updated {
		return
	}
	dp.config.Peers = append(dp.config.Peers, req.AddPeer)
	addr := strings.Split(req.AddPeer.Addr, ":")[0]
	dp.config.RaftStore.AddNodeWithPort(req.AddPeer.ID, addr, heartbeatPort, replicatePort)
	return
}

func (dp *dataPartition) confRemoveNode(req *proto.DataPartitionOfflineRequest, index uint64) (updated bool, err error) {
	if dp.raftPartition == nil {
		err = fmt.Errorf("%s partitionId=%v applyid=%v", RaftIsNotStart, dp.partitionId, index)
		return
	}
	peerIndex := -1
	for i, peer := range dp.config.Peers {
		if peer.ID == req.RemovePeer.ID {
			updated = true
			peerIndex = i
			break
		}
	}
	if !updated {
		return
	}
	if req.RemovePeer.ID == dp.config.NodeId {
		go func(index uint64) {
			for {
				time.Sleep(time.Millisecond)
				if dp.raftPartition.AppliedIndex() < index {
					continue
				}
				if dp.raftPartition != nil {
					dp.raftPartition.Delete()
				}
				dp.Stop()
				os.RemoveAll(dp.Path())
				log.LogDebugf("[confRemoveNode]: remove self end.")
				return
			}
		}(index)
		updated = false
		log.LogDebugf("[confRemoveNode]: begin remove self.")
		return
	}
	dp.config.Peers = append(dp.config.Peers[:peerIndex], dp.config.Peers[peerIndex+1:]...)
	log.LogDebugf("[confRemoveNode]: remove peer.")
	return
}

func (dp *dataPartition) confUpdateNode(req *proto.DataPartitionOfflineRequest, index uint64) (updated bool, err error) {
	log.LogDebugf("[confUpdateNode]: not support.")
	return
}

func (dp *dataPartition) storeApplyIndex(applyIndex uint64) (err error) {
	filename := path.Join(dp.Path(), TempApplyIndexFile)
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_TRUNC|os.O_CREATE, 0755)
	if err != nil {
		return
	}
	defer func() {
		fp.Sync()
		fp.Close()
		os.Remove(filename)
	}()
	if _, err = fp.WriteString(fmt.Sprintf("%d", applyIndex)); err != nil {
		return
	}
	err = os.Rename(filename, path.Join(dp.Path(), ApplyIndexFile))
	return
}

func (dp *dataPartition) LoadApplyIndex() (err error) {
	filename := path.Join(dp.Path(), ApplyIndexFile)
	if _, err = os.Stat(filename); err != nil {
		err = nil
		return
	}
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		if err == os.ErrNotExist {
			err = nil
			return
		}
		err = errors.Errorf("[loadApplyIndex] OpenFile: %s", err.Error())
		return
	}
	if len(data) == 0 {
		err = errors.Errorf("[loadApplyIndex]: ApplyIndex is empty")
		return
	}
	if _, err = fmt.Sscanf(string(data), "%d", &dp.applyId); err != nil {
		err = errors.Errorf("[loadApplyID] ReadApplyID: %s", err.Error())
		return
	}
	return
}

func (dp *dataPartition) SetMinAppliedId(id uint64) {
	dp.minAppliedId = id
}

func (dp *dataPartition) GetAppliedId() (id uint64) {
	return dp.applyId
}

//random write need start raft server
func (s *DataNode) parseRaftConfig(cfg *config.Config) (err error) {
	s.raftDir = cfg.GetString(ConfigKeyRaftDir)
	if s.raftDir == "" {
		s.raftDir = DefaultRaftDir
	}
	s.raftHeartbeat = cfg.GetString(ConfigKeyRaftHeartbeat)
	s.raftReplicate = cfg.GetString(ConfigKeyRaftReplicate)

	log.LogDebugf("[parseRaftConfig] load raftDir[%v].", s.raftDir)
	log.LogDebugf("[parseRaftConfig] load raftHearbeat[%v].", s.raftHeartbeat)
	log.LogDebugf("[parseRaftConfig] load raftReplicate[%v].", s.raftReplicate)
	return
}

func (s *DataNode) startRaftServer(cfg *config.Config) (err error) {
	log.LogInfo("Start: startRaftServer")

	s.parseRaftConfig(cfg)

	if _, err = os.Stat(s.raftDir); err != nil {
		if err = os.MkdirAll(s.raftDir, 0755); err != nil {
			err = errors.Errorf("create raft server dir: %s", err.Error())
			log.LogErrorf("action[startRaftServer] cannot start raft server err[%v]", err)
			return
		}
	}

	heartbeatPort, _ := strconv.Atoi(s.raftHeartbeat)
	replicatePort, _ := strconv.Atoi(s.raftReplicate)

	raftConf := &raftstore.Config{
		NodeID:        s.nodeId,
		WalPath:       s.raftDir,
		IpAddr:        s.localIp,
		HeartbeatPort: heartbeatPort,
		ReplicatePort: replicatePort,
	}
	s.raftStore, err = raftstore.NewRaftStore(raftConf)
	if err != nil {
		err = errors.Errorf("new raftStore: %s", err.Error())
		log.LogErrorf("action[startRaftServer] cannot start raft server err[%v]", err)
	}

	return
}

func (s *DataNode) stopRaftServer() {
	if s.raftStore != nil {
		s.raftStore.Stop()
	}
}

func (dp *dataPartition) ExtentRepair(extentFiles []*storage.FileInfo) {
	startTime := time.Now().UnixNano()
	log.LogInfof("action[ExtentRepair] partition=%v start.", dp.partitionId)

	mf := NewMemberFileMetas()

	for i := 0; i < len(extentFiles); i++ {
		extentFile := extentFiles[i]
		addFile := &storage.FileInfo{Source: extentFile.Source, FileId: extentFile.FileId, Size: extentFile.Size, Inode: extentFile.Inode}
		mf.NeedAddExtentsTasks = append(mf.NeedAddExtentsTasks, addFile)
		log.LogInfof("action[ExtentRepair] partition=%v addFile[%v].", dp.partitionId, addFile)
	}

	dp.MergeRepair(mf)

	finishTime := time.Now().UnixNano()
	log.LogInfof("action[ExtentRepair] partition=%v finish cost[%vms].",
		dp.partitionId, (finishTime-startTime)/int64(time.Millisecond))
}

func (dp *dataPartition) ApplyErrRepair(extentId uint64) {
	extentFiles := make([]*storage.FileInfo, 0)
	leaderAddr, isLeader := dp.IsLeader()
	dp.stopRaft()
	log.LogWarn("action[ExtentRepair] stop raft partition=%v", dp.partitionId)
	extentInfo, err := dp.extentStore.GetOneWatermark(extentId, storage.GetStableExtentFilter())
	if err != nil {
		err = errors.Annotatef(err, "getAllMemberFileMetas extent dataPartition[%v] GetAllWaterMark", dp.partitionId)
		return
	}

	if isLeader {
		// If leader apply error, notify all follower to start repair
		memberMetas := NewMemberFileMetas()
		memberMetas.files[extentInfo.FileId] = extentInfo
		addFile := &storage.FileInfo{Source: leaderAddr, FileId: extentInfo.FileId, Size: extentInfo.Size, Inode: extentInfo.Inode}
		memberMetas.NeedAddExtentsTasks = append(memberMetas.NeedAddExtentsTasks, addFile)
		memberMetas.TaskType = FixRaftFollower
		err = dp.NotifyRaftFollowerRepair(memberMetas)
		if err != nil {
			log.LogErrorf("action[ExtentRepair] err: %v", err)
		}
		log.LogInfof("action[ExtentRepair] leader repair follower partition=%v addFile[%v].", dp.partitionId, addFile)

	} else if leaderAddr != "" {
		// If follower apply error, delete local extent and repair from leader
		dp.extentStore.DeleteDirtyExtent(extentId)
		log.LogWarnf("action[ExtentRepair] Delete follower dirty extent [%v_%v]", dp.partitionId, extentId)

		// Repair local extent
		extentInfo.Source = leaderAddr
		extentFiles = append(extentFiles, extentInfo)
		dp.ExtentRepair(extentFiles)
	}
}

// Get all files meta
func (dp *dataPartition) getFileMetas(targetAddr string) (extentFiles []*storage.FileInfo, err error) {
	// get remote files meta by opGetAllWaterMarker cmd
	p := NewGetAllWaterMarker(dp.partitionId)
	var conn *net.TCPConn
	target := targetAddr
	conn, err = gConnPool.Get(target) //get remote connect
	if err != nil {
		err = errors.Annotatef(err, "getFileMetas  partition=%v get host[%v] connect", dp.partitionId, target)
		return
	}
	err = p.WriteToConn(conn) //write command to remote host
	if err != nil {
		gConnPool.Put(conn, true)
		err = errors.Annotatef(err, "getFileMetas partition=%v write to host[%v]", dp.partitionId, target)
		return
	}
	err = p.ReadFromConn(conn, 60) //read it response
	if err != nil {
		gConnPool.Put(conn, true)
		err = errors.Annotatef(err, "getFileMetas partition=%v read from host[%v]", dp.partitionId, target)
		return
	}
	fileInfos := make([]*storage.FileInfo, 0)
	err = json.Unmarshal(p.Data[:p.Size], &fileInfos)
	if err != nil {
		gConnPool.Put(conn, true)
		err = errors.Annotatef(err, "getFileMetas partition=%v unmarshal json[%v]", dp.partitionId, string(p.Data[:p.Size]))
		return
	}

	extentFiles = make([]*storage.FileInfo, 0)
	for _, fileInfo := range fileInfos {
		extentFiles = append(extentFiles, fileInfo)
	}

	gConnPool.Put(conn, true)

	return
}

func NewGetAppliedId(partitionId uint32, minAppliedId uint64) (p *Packet) {
	p = new(Packet)
	p.Opcode = proto.OpGetAppliedId
	p.PartitionID = partitionId
	p.Magic = proto.ProtoMagic
	p.ReqID = proto.GetReqID()
	p.Data = make([]byte, 8)
	binary.BigEndian.PutUint64(p.Data, minAppliedId)
	p.Size = uint32(len(p.Data))
	return
}

// Get all files meta
func (dp *dataPartition) getMinAppliedId() {
	//only first host get applied
	if strings.Split(dp.replicaHosts[0], ":")[0] != LocalIP {
		return
	}
	var (
		minAppliedId uint64
		err          error
	)

	defer func(newMinAppliedId uint64) {
		if err == nil {
			log.LogDebugf("[getMinAppliedId] partition=%v success old minAppId=%v newAppId=%v",
				dp.partitionId, dp.minAppliedId, newMinAppliedId)
			//success maybe update the minAppliedId
			dp.minAppliedId = newMinAppliedId
		} else {
			//do nothing
			log.LogErrorf("[getMinAppliedId] partition=%v newAppId=%v err %v",
				dp.partitionId, newMinAppliedId, err)
		}
	}(minAppliedId)

	// send the last minAppliedId and get current appliedId
	p := NewGetAppliedId(dp.partitionId, dp.minAppliedId)
	minAppliedId = dp.applyId
	for i := 1; i < len(dp.replicaHosts); i++ {
		var conn *net.TCPConn
		target := dp.replicaHosts[i]
		conn, err = gConnPool.Get(target) //get remote connect
		if err != nil {
			err = errors.Annotatef(err, "getMinAppliedId  partition=%v get host[%v] connect", dp.partitionId, target)
			return
		}

		err = p.WriteToConn(conn) //write command to remote host
		if err != nil {
			gConnPool.Put(conn, true)
			err = errors.Annotatef(err, "getMinAppliedId partition=%v write to host[%v]", dp.partitionId, target)
			return
		}
		err = p.ReadFromConn(conn, 60) //read it response
		if err != nil {
			gConnPool.Put(conn, true)
			err = errors.Annotatef(err, "getMinAppliedId partition=%v read from host[%v]", dp.partitionId, target)
			return
		}

		remoteAppliedId := binary.BigEndian.Uint64(p.Data)
		if remoteAppliedId < minAppliedId && remoteAppliedId != 0 {
			minAppliedId = remoteAppliedId
		}
		gConnPool.Put(conn, true)
	}
	return
}
