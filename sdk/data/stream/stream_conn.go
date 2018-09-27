package stream

import (
	"fmt"
	"net"
	"time"

	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/sdk/data/wrapper"
	"github.com/tiglabs/containerfs/util/log"
	"github.com/tiglabs/containerfs/util/pool"
)

var (
	NotLeaderError = errors.New("NotLeaderError")
)

const (
	StreamSendMaxRetry      = 100
	StreamSendSleepInterval = 100 * time.Millisecond
)

type GetReplyFunc func(conn *net.TCPConn) (err error, again bool)

type StreamConn struct {
	partition uint32
	currAddr  string
	hosts     []string
}

var (
	StreamConnPool = pool.NewConnPool()
)

func NewStreamConn(dp *wrapper.DataPartition) *StreamConn {
	return &StreamConn{
		partition: dp.PartitionID,
		currAddr:  dp.LeaderAddr,
		hosts:     dp.Hosts,
	}
}

func (sc *StreamConn) String() string {
	return fmt.Sprintf("Partition(%v) CurrentAddr(%v) Hosts(%v)", sc.partition, sc.currAddr, sc.hosts)
}

func (sc *StreamConn) Send(req *Packet, getReply GetReplyFunc) (err error) {
	for i := 0; i < StreamSendMaxRetry; i++ {
		err = sc.sendToPartition(req, getReply)
		if err == nil {
			return
		}
		log.LogWarnf("StreamConn Send: err(%v)", err)
	}
	return errors.New(fmt.Sprintf("StreamConn Send: retried %v times and still failed, sc(%v) req(%v)", StreamSendMaxRetry, sc, req))
}

func (sc *StreamConn) sendToPartition(req *Packet, getReply GetReplyFunc) (err error) {
	conn, err := StreamConnPool.Get(sc.currAddr)
	if err == nil {
		err = sc.sendToConn(conn, req, getReply)
		if err == nil {
			StreamConnPool.Put(conn, false)
			return
		}
		log.LogWarnf("sendToPartition: curr addr failed, addr(%v) req(%v) err(%v)", sc.currAddr, req, err)
		StreamConnPool.Put(conn, true)
		if err != NotLeaderError {
			return
		}
	}

	for _, addr := range sc.hosts {
		log.LogWarnf("sendToPartition: try addr(%v) req(%v)", addr, req)
		conn, err = StreamConnPool.Get(addr)
		if err != nil {
			log.LogWarnf("sendToPartition: failed to get connection to addr(%v) req(%v) err(%v)", addr, req, err)
			continue
		}
		sc.currAddr = addr
		err = sc.sendToConn(conn, req, getReply)
		if err == nil {
			StreamConnPool.Put(conn, false)
			return
		}
		StreamConnPool.Put(conn, true)
		if err != NotLeaderError {
			return
		}
	}
	return errors.New(fmt.Sprintf("sendToPatition Failed: sc(%v) req(%v)", sc, req))
}

func (sc *StreamConn) sendToConn(conn *net.TCPConn, req *Packet, getReply GetReplyFunc) (err error) {
	for i := 0; i < StreamSendMaxRetry; i++ {
		log.LogDebugf("sendToConn: send to addr(%v), req(%v)", sc.currAddr, req)
		err = req.WriteToConn(conn)
		if err != nil {
			msg := fmt.Sprintf("sendToConn: failed to write to addr(%v) err(%v)", sc.currAddr, err)
			log.LogWarn(msg)
			break
		}

		var again bool
		err, again = getReply(conn)
		if !again {
			if err != nil {
				log.LogWarnf("sendToConn: getReply error and RETURN, addr(%v) req(%v) err(%v)", sc.currAddr, req, err)
			}
			break
		}

		log.LogWarnf("sendToConn: getReply error and will RETRY, sc(%v) err(%v)", sc, err)
		time.Sleep(StreamSendSleepInterval)
	}

	log.LogDebugf("sendToConn exit: send to addr(%v) req(%v) err(%v)", sc.currAddr, req, err)
	return
}
