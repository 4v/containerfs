// Copyright 2018 The Containerfs Authors.
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

package proto

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/tiglabs/containerfs/util/btree"
)

var InvalidKey = errors.New("invalid key error")

type ExtentKey struct {
	FileOffset  uint64
	PartitionId uint32
	ExtentId    uint64
	Size        uint32
	Crc         uint32
}

func (k *ExtentKey) String() string {
	return fmt.Sprintf("ExtentKey{FileOffset(%v),Partition(%v),ExtentID(%v),Size(%v),CRC(%v)}", k.FileOffset, k.PartitionId, k.ExtentId, k.Size, k.Crc)
}

func (k *ExtentKey) Less(than btree.Item) bool {
	that := than.(*ExtentKey)
	return k.FileOffset < that.FileOffset
}

func (k *ExtentKey) Marshal() (m string) {
	return fmt.Sprintf("%v_%v_%v_%v_%v", k.FileOffset, k.PartitionId, k.ExtentId,
		k.Size,k.Crc)
}

func (k *ExtentKey) MarshalBinary() ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0,28))
	if err := binary.Write(buf, binary.BigEndian, k.FileOffset); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, k.PartitionId); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, k.ExtentId); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, k.Size); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, k.Crc); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (k *ExtentKey) UnmarshalBinary(buf *bytes.Buffer) (err error) {
	if err = binary.Read(buf, binary.BigEndian, &k.FileOffset); err != nil {
		return
	}
	if err = binary.Read(buf, binary.BigEndian, &k.PartitionId); err != nil {
		return
	}
	if err = binary.Read(buf, binary.BigEndian, &k.ExtentId); err != nil {
		return
	}
	if err = binary.Read(buf, binary.BigEndian, &k.Size); err != nil {
		return
	}
	if err = binary.Read(buf, binary.BigEndian, &k.Crc); err != nil {
		return
	}
	return
}

func (k *ExtentKey) GetExtentKey() (m string) {
	return fmt.Sprintf("%v", k.FileOffset)
}

func (k *ExtentKey) UnMarshal(m string) (err error) {
	_, err = fmt.Sscanf(m, "%v_%v_%v_%v_%v", &k.FileOffset, &k.PartitionId,
		&k.ExtentId, &k.Size, &k.Crc)
	return
}
