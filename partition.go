package schema

import (
	"encoding/binary"
	"hash/fnv"
	"sort"

	"github.com/cespare/xxhash"
	jump "github.com/dgryski/go-jump"
)

type PartitionByMethod uint8

const (
	PartitionByOrg PartitionByMethod = iota
	PartitionBySeries
	PartitionBySeriesWithTags
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
		h.WriteString(m.Name)
		sort.Strings(m.Tags)
		for _, t := range m.Tags {
			if len(t) >= 5 && t[:5] == "name=" {
				continue
			}

			h.WriteString(";")
			h.WriteString(t)
		}
		partition = jump.Hash(h.Sum64(), int(partitions))
	case PartitionBySeriesWithTagsFnv:
		h := fnv.New32a()
		h.Write([]byte(m.Name))
		sort.Strings(m.Tags)
		for _, t := range m.Tags {
			if len(t) >= 5 && t[:5] == "name=" {
				continue
			}

			h.Write([]byte(";"))
			h.Write([]byte(t))
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
			h.WriteString(m.Name)
			sort.Strings(m.Tags)
			for _, t := range m.Tags {
				if len(t) >= 5 && t[:5] == "name=" {
					continue
				}

				h.WriteString(";")
				h.WriteString(t)
			}
		}
		partition = jump.Hash(h.Sum64(), int(partitions))
	case PartitionBySeriesWithTagsFnv:
		h := fnv.New32a()
		if len(m.nameWithTags) > 0 {
			h.Write([]byte(m.nameWithTags))
		} else {
			h.Write([]byte(m.Name))
			sort.Strings(m.Tags)
			for _, t := range m.Tags {
				if len(t) >= 5 && t[:5] == "name=" {
					continue
				}

				h.Write([]byte(";"))
				h.Write([]byte(t))
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
