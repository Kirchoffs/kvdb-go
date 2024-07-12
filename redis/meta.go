package redis

import (
	"encoding/binary"
	"math"
)

const (
    maxMetadataSize = 1 + 2 * binary.MaxVarintLen64 + binary.MaxVarintLen32
    extraListMetadataSize = 2 * binary.MaxVarintLen32
    initialListMark = math.MaxUint64 / 2
)

type metadata struct {
    dataType byte
    expire int64
    version int64
    size uint32
    head uint64
    tail uint64
}

func (md *metadata) encode() []byte {
    var size = maxMetadataSize
    if md.dataType == List {
        size += extraListMetadataSize
    }

    buffer := make([]byte, size)
    buffer[0] = md.dataType
    index := 1
    index += binary.PutVarint(buffer[index:], md.expire)
    index += binary.PutVarint(buffer[index:], md.version)
    index += binary.PutUvarint(buffer[index:], uint64(md.size))

    if md.dataType == List {
        index += binary.PutUvarint(buffer[index:], uint64(md.head))
        index += binary.PutUvarint(buffer[index:], uint64(md.tail))
    }

    return buffer[:index]
}

func decode(data []byte) *metadata {
    dataType := data[0]
    index := 1

    expire, n := binary.Varint(data[index:])
    index += n

    version, n := binary.Varint(data[index:])
    index += n

    size, n := binary.Uvarint(data[index:])
    index += n

    var head uint64 = 0
    var tail uint64 = 0
    if dataType == List {
        head, n = binary.Uvarint(data[index:])
        index += n

        tail, _ = binary.Uvarint(data[index:])
    }

    return &metadata{
        dataType: dataType,
        expire: expire,
        version: version,
        size: uint32(size),
        head: head,
        tail: tail,
    }
}

type hashInternalKey struct {
    key []byte
    version int64
    field []byte
}

func (hk *hashInternalKey) encode() []byte {
    var size = len(hk.key) + 8 + len(hk.field)
    buffer := make([]byte, size)
    index := 0
    
    copy(buffer[index:], hk.key)
    index += len(hk.key)

    binary.LittleEndian.PutUint64(buffer[index:], uint64(hk.version))
    index += 8

    copy(buffer[index:], hk.field)

    return buffer
}

type setInternalKey struct {
    key []byte
    version int64
    member []byte
}

func (hk *setInternalKey) encode() []byte {
    var size = len(hk.key) + 8 + len(hk.member)
    buffer := make([]byte, size)
    index := 0
    
    copy(buffer[index:], hk.key)
    index += len(hk.key)

    binary.LittleEndian.PutUint64(buffer[index:], uint64(hk.version))
    index += 8

    copy(buffer[index:], hk.member)

    return buffer
}

type listInternalKey struct {
    key []byte
    version int64
    index uint64
}

func (lk *listInternalKey) encode() []byte {
    var size = len(lk.key) + 8 + 8
    buffer := make([]byte, size)
    index := 0
    
    copy(buffer[index:], lk.key)
    index += len(lk.key)

    binary.LittleEndian.PutUint64(buffer[index:], uint64(lk.version))
    index += 8

    binary.LittleEndian.PutUint64(buffer[index:], lk.index)

    return buffer
}
