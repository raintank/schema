package schema

import (
	"bytes"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync/atomic"
)

var ErrInvalidIntervalzero = errors.New("interval cannot be 0")
var ErrInvalidOrgIdzero = errors.New("org-id cannot be 0")
var ErrInvalidEmptyName = errors.New("name cannot be empty")
var ErrInvalidMtype = errors.New("invalid mtype")
var ErrInvalidTagFormat = errors.New("invalid tag format")
var ErrUnknownPartitionMethod = errors.New("unknown partition method")

type PartitionedMetric interface {
	Validate() error
	SetId()
	// PartitionID returns the partition id that should be used for this metric.
	PartitionID(method PartitionByMethod, partitions int32) (int32, error)
}

//go:generate msgp

// MetricData contains all metric metadata (some as fields, some as tags) and a datapoint
type MetricData struct {
	Id       string   `json:"id"`
	OrgId    int      `json:"org_id"`
	Name     string   `json:"name"`
	Interval int      `json:"interval"`
	Value    float64  `json:"value"`
	Unit     string   `json:"unit"`
	Time     int64    `json:"time"`
	Mtype    string   `json:"mtype"`
	Tags     []string `json:"tags"`
}

func (m *MetricData) Validate() error {
	if m.OrgId == 0 {
		return ErrInvalidOrgIdzero
	}
	if m.Interval == 0 {
		return ErrInvalidIntervalzero
	}
	if m.Name == "" {
		return ErrInvalidEmptyName
	}
	if m.Mtype == "" || (m.Mtype != "gauge" && m.Mtype != "rate" && m.Mtype != "count" && m.Mtype != "counter" && m.Mtype != "timestamp") {
		return ErrInvalidMtype
	}
	if !ValidateTags(m.Tags) {
		return ErrInvalidTagFormat
	}
	return nil
}

// returns a id (hash key) in the format OrgId.md5Sum
// the md5sum is a hash of the the concatination of the
// metric + each tag key:value pair (in metrics2.0 sense, so also fields), sorted alphabetically.
func (m *MetricData) SetId() {
	sort.Strings(m.Tags)

	buffer := bytes.NewBufferString(m.Name)
	buffer.WriteByte(0)
	buffer.WriteString(m.Unit)
	buffer.WriteByte(0)
	buffer.WriteString(m.Mtype)
	buffer.WriteByte(0)
	fmt.Fprintf(buffer, "%d", m.Interval)

	for _, k := range m.Tags {
		buffer.WriteByte(0)
		buffer.WriteString(k)
	}
	m.Id = fmt.Sprintf("%d.%x", m.OrgId, md5.Sum(buffer.Bytes()))
}

// can be used by some encoders, such as msgp
type MetricDataArray []*MetricData

type MetricDefinition struct {
	Id       MKey   `json:"mkey"`
	OrgId    uint32 `json:"org_id"`
	Name     string `json:"name"`
	Interval int    `json:"interval"`
	Unit     string `json:"unit"`
	Mtype    string `json:"mtype"`

	// some users of MetricDefinition (f.e. MetricTank) might add a "name" tag
	// to this slice which allows querying by name as a tag. this special tag
	// should not be stored or transmitted over the network, otherwise it may
	// just get overwritten by the receiver.
	Tags       []string `json:"tags"`
	LastUpdate int64    `json:"lastUpdate"` // unix timestamp
	Partition  int32    `json:"partition"`

	// this is a special attribute that does not need to be set, it is only used
	// to keep the state of NameWithTags()
	nameWithTags string
}

// NameWithTags deduplicates the name and tags strings by storing their content
// as a single string in .nameWithTags and then makes .Name and the .Tags slices
// of it. Once this has been done it won't do it a second time, but just reuse
// the already generated .nameWithTags.
// It returns the deduplicated name with tags.
func (m *MetricDefinition) NameWithTags() string {
	if len(m.nameWithTags) > 0 {
		return m.nameWithTags
	}

	nameWithTagsBuffer := &bytes.Buffer{}
	_ = writeSortedTagString(nameWithTagsBuffer, m.Name, m.Tags)
	m.nameWithTags = nameWithTagsBuffer.String()

	var i int
	cursor := len(m.Name)
	m.Name = m.nameWithTags[:cursor]
	for _, t := range m.Tags {
		if len(t) > 5 && t[:5] == "name=" {
			continue
		}
		m.Tags[i] = m.nameWithTags[cursor+1 : cursor+1+len(t)]
		cursor += len(t) + 1
		i++
	}

	// if a "name" tag existed, then we have to shorten the slice
	if i < len(m.Tags) {
		m.Tags = m.Tags[:i]
	}

	return m.nameWithTags
}

// Clone() returns a copy of the MetricDefinition. It uses atomic operations
// to read certain properties that get updated atomically
func (m *MetricDefinition) Clone() MetricDefinition {
	return MetricDefinition{
		Id:           m.Id,
		OrgId:        m.OrgId,
		Name:         m.Name,
		Interval:     m.Interval,
		Unit:         m.Unit,
		Mtype:        m.Mtype,
		Tags:         m.Tags,
		LastUpdate:   atomic.LoadInt64(&m.LastUpdate),
		Partition:    atomic.LoadInt32(&m.Partition),
		nameWithTags: m.nameWithTags,
	}
}

func (m *MetricDefinition) NameSanitizedAsTagValue() string {
	return SanitizeNameAsTagValue(m.Name)
}

func (m *MetricDefinition) SetId() {
	sort.Strings(m.Tags)

	buffer := bytes.NewBufferString(m.Name)
	buffer.WriteByte(0)
	buffer.WriteString(m.Unit)
	buffer.WriteByte(0)
	buffer.WriteString(m.Mtype)
	buffer.WriteByte(0)
	fmt.Fprintf(buffer, "%d", m.Interval)

	for _, t := range m.Tags {
		if len(t) > 5 && t[:5] == "name=" {
			continue
		}

		buffer.WriteByte(0)
		buffer.WriteString(t)
	}

	m.Id = MKey{
		md5.Sum(buffer.Bytes()),
		uint32(m.OrgId),
	}
}

func (m *MetricDefinition) Validate() error {
	if m.OrgId == 0 {
		return ErrInvalidOrgIdzero
	}
	if m.Interval == 0 {
		return ErrInvalidIntervalzero
	}
	if m.Name == "" {
		return ErrInvalidEmptyName
	}
	if m.Mtype == "" || (m.Mtype != "gauge" && m.Mtype != "rate" && m.Mtype != "count" && m.Mtype != "counter" && m.Mtype != "timestamp") {
		return ErrInvalidMtype
	}
	if !ValidateTags(m.Tags) {
		return ErrInvalidTagFormat
	}
	return nil
}

// MetricDefinitionFromMetricData yields a MetricDefinition that has no references
// to the original MetricData
func MetricDefinitionFromMetricData(d *MetricData) *MetricDefinition {
	tags := make([]string, len(d.Tags))
	copy(tags, d.Tags)
	mkey, _ := MKeyFromString(d.Id)

	md := &MetricDefinition{
		Id:         mkey,
		Name:       d.Name,
		OrgId:      uint32(d.OrgId),
		Mtype:      d.Mtype,
		Interval:   d.Interval,
		LastUpdate: d.Time,
		Unit:       d.Unit,
		Tags:       tags,
	}

	return md
}

// SanitizeNameAsTagValue takes a name and potentially
// modifies it to ensure it is a valid value that can be
// used as a tag value. This is important when we index
// metric names as values of the tag "name"
func SanitizeNameAsTagValue(name string) string {
	if len(name) == 0 || name[0] != '~' {
		return name
	}

	for i := 1; i < len(name); i++ {
		if name[i] != '~' {
			return name[i:]
		}
	}

	// the whole name consists of no other chars than '~'
	return ""
}

// EatDots removes multiple consecutive, leading, and trailing dots
// from name. If the provided name is only dots, it will return an
// empty string
func EatDots(name string) string {
	if len(name) == 0 {
		return ""
	}

	n := strings.Trim(name, ".")

	ln := len(n)

	if ln == 0 {
		return ""
	}

	rt := make([]byte, 0, ln)

	for i, s := range n {

		if s == '.' {
			// since trailing dots are already removed we won't
			// ever hit an index out of range
			if n[i+1] == '.' {
				continue
			}

			// if this is the last dot in a consecutive series, append it
			rt = append(rt, '.')
			continue
		}

		// if it is not a dot, append it
		rt = append(rt, byte(s))
	}

	// return a string representation of our clean byte slice
	return string(rt)
}

// ValidateTags returns whether all tags are in a valid format.
// a valid format is anything that looks like key=value,
// the length of key and value must be >0 and both cannot contain
// the certain prohibited characters
func ValidateTags(tags []string) bool {
	for _, t := range tags {
		if !ValidateTag(t) {
			return false
		}
	}

	return true
}

func ValidateTag(tag string) bool {
	// a valid tag must have:
	// - a key that's at least 1 char long
	// - a = sign
	// - a value that's at least 1 char long
	if len(tag) < 3 {
		return false
	}

	equal := strings.Index(tag, "=")
	if equal == -1 {
		return false
	}

	// first equal sign must not be the first nor last character
	if equal == 0 || equal == len(tag)-1 {
		return false
	}

	return ValidateTagKey(tag[:equal]) && ValidateTagValue(tag[equal+1:])
}

// ValidateTagKey validates tag key requirements as defined in graphite docs
func ValidateTagKey(key string) bool {
	if len(key) == 0 {
		return false
	}

	return !strings.ContainsAny(key, ";!^=")
}

// ValidateTagValue is the same as the above ValidateTagKey, but for the tag value
func ValidateTagValue(value string) bool {
	if len(value) == 0 {
		return false
	}

	if value[0] == '~' {
		return false
	}

	return !strings.ContainsRune(value, ';')
}

func writeSortedTagString(w io.Writer, name string, tags []string) error {
	sort.Strings(tags)

	_, err := io.WriteString(w, name)
	if err != nil {
		return err
	}

	for _, t := range tags {
		if len(t) > 5 && t[:5] == "name=" {
			continue
		}

		_, err = io.WriteString(w, ";")
		if err != nil {
			return err
		}

		_, err = io.WriteString(w, t)
		if err != nil {
			return err
		}
	}

	return nil
}
