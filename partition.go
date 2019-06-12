package schema

import (
	"encoding/binary"
	"hash/fnv"

	"github.com/cespare/xxhash"
	jump "github.com/dgryski/go-jump"
)

type PartitionByMethod uint8

const (
	// partition by organization, ignoring all other properties
	PartitionByOrg PartitionByMethod = iota

	// partition by the metric name, without taking the tags into account
	PartitionBySeries

	// the recommended partitioning scheme to be used whenever possible, because it results in a good distribution
	PartitionBySeriesWithTags

	// kept for backwards compatibility, but not recommended
	PartitionBySeriesWithTagsFnv
)

func (m *MetricData) PartitionID(method PartitionByMethod, partitions int32) (int32, error) {
	var partition int32

	switch method {
	case PartitionByOrg:
		h := fnv.New32a()
		err := binary.Write(h, binary.LittleEndian, uint32(m.OrgId))
		if err != nil {
			return 0, err
		}
		partition = int32(h.Sum32()) % partitions
		if partition < 0 {
			partition = -partition
		}
	case PartitionBySeries:
		h := fnv.New32a()
		h.Write([]byte(m.Name))
		partition = int32(h.Sum32()) % partitions
		if partition < 0 {
			partition = -partition
		}
	case PartitionBySeriesWithTags:
		h := xxhash.New()
		if err := writeSortedTagString(h, m.Name, m.Tags); err != nil {
			return 0, err
		}
		partition = jump.Hash(h.Sum64(), int(partitions))
	case PartitionBySeriesWithTagsFnv:
		h := fnv.New32a()
		if err := writeSortedTagString(h, m.Name, m.Tags); err != nil {
			return 0, err
		}
		partition = int32(h.Sum32()) % partitions
		if partition < 0 {
			partition = -partition
		}
	default:
		return 0, ErrUnknownPartitionMethod
	}

	return partition, nil
}

func (m *MetricDefinition) PartitionID(method PartitionByMethod, partitions int32) (int32, error) {
	var partition int32

	switch method {
	case PartitionByOrg:
		h := fnv.New32a()
		err := binary.Write(h, binary.LittleEndian, uint32(m.OrgId))
		if err != nil {
			return 0, err
		}
		partition = int32(h.Sum32()) % partitions
		if partition < 0 {
			partition = -partition
		}
	case PartitionBySeries:
		h := fnv.New32a()
		h.Write([]byte(m.Name))
		partition = int32(h.Sum32()) % partitions
		if partition < 0 {
			partition = -partition
		}
	case PartitionBySeriesWithTags:
		h := xxhash.New()
		if len(m.nameWithTags) > 0 {
			h.WriteString(m.nameWithTags)
		} else {
			if err := writeSortedTagString(h, m.Name, m.Tags); err != nil {
				return 0, err
			}
		}
		partition = jump.Hash(h.Sum64(), int(partitions))
	case PartitionBySeriesWithTagsFnv:
		h := fnv.New32a()
		if len(m.nameWithTags) > 0 {
			h.Write([]byte(m.nameWithTags))
		} else {
			if err := writeSortedTagString(h, m.Name, m.Tags); err != nil {
				return 0, err
			}
		}
		partition = int32(h.Sum32()) % partitions
		if partition < 0 {
			partition = -partition
		}
	default:
		return 0, ErrUnknownPartitionMethod
	}

	return partition, nil
}
