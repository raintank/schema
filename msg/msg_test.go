package msg

import (
	"math/rand"
	"strconv"
	"testing"

	schemaV1 "gopkg.in/raintank/schema.v1"
	schema "gopkg.in/raintank/schema.v2"
)

func generateMetricData(amount int) []schema.DataPoint {
	names := []string{
		"litmus.http.error_state.",
		"litmus.hello.dieter_plaetinck.be",
		"litmus.ok.raintank_dns_error_state_foo_longer",
		"hi.alerting.state",
	}
	intervals := []int{1, 10, 60}
	tags := [][]string{
		{
			"foo=bar",
			"endpoint_id=25",
			"collector_id=hi",
		},
		{
			"foo_bar=quux",
			"endpoint_id=25",
			"collector_id=hi",
			"some_other_tag=ok",
		},
	}
	r := rand.New(rand.NewSource(438))
	out := make([]schema.DataPoint, amount)
	for i := 0; i < amount; i++ {
		m := &schema.MetricData{
			MetricMetadata: schema.MetricMetadata{
				OrgId:    i,
				Name:     names[i%len(names)] + "foo.bar" + strconv.Itoa(i),
				Interval: intervals[i%len(intervals)],
				Unit:     "foo",
				Mtype:    "bleh",
				Tags:     tags[i%len(tags)],
			},
			Point: schema.Point{
				Val: r.Float64(),
				Ts:  r.Uint32(),
			},
		}
		m.SetId()
		out[i] = m

	}
	return out
}

func generateMetricDataV1Message(amount int) [][]byte {
	names := []string{
		"litmus.http.error_state.",
		"litmus.hello.dieter_plaetinck.be",
		"litmus.ok.raintank_dns_error_state_foo_longer",
		"hi.alerting.state",
	}
	intervals := []int{1, 10, 60}
	tags := [][]string{
		{
			"foo=bar",
			"endpoint_id=25",
			"collector_id=hi",
		},
		{
			"foo_bar=quux",
			"endpoint_id=25",
			"collector_id=hi",
			"some_other_tag=ok",
		},
	}
	r := rand.New(rand.NewSource(438))
	points := make([]*schemaV1.MetricData, amount)
	for i := 0; i < amount; i++ {
		m := &schemaV1.MetricData{
			OrgId:    i,
			Name:     names[i%len(names)] + "foo.bar" + strconv.Itoa(i),
			Interval: intervals[i%len(intervals)],
			Unit:     "foo",
			Mtype:    "bleh",
			Tags:     tags[i%len(tags)],
			Value:    r.Float64(),
			Time:     r.Int63(),
		}
		m.SetId()
		points[i] = m

	}
	out := make([][]byte, len(points))
	var err error
	for i, p := range points {
		out[i], err = p.MarshalMsg(nil)
		if err != nil {
			panic(err)
		}
	}
	return out
}

func generateMetricPoint(n int) []schema.DataPoint {
	datas := generateMetricData(n)
	points := make([]schema.DataPoint, n)
	for i, md := range datas {
		points[i] = &schema.MetricPoint{
			Id:    md.GetId(),
			Point: md.(*schema.MetricData).Point,
		}
	}

	return points
}

func generateDataPointMessages(points []schema.DataPoint) [][]byte {
	messages := make([][]byte, len(points))
	for i, p := range points {
		message, err := EncodeDataPoint(p, nil)
		if err != nil {
			panic(err)
		}
		messages[i] = message
	}
	return messages
}

func BenchmarkDecodeMetricData(b *testing.B) {
	numPoints := b.N
	if numPoints > 3000 {
		numPoints = 3000
	}
	msgs := generateDataPointMessages(generateMetricData(numPoints))
	b.ResetTimer()
	var err error
	for i := 0; i < b.N; i++ {
		_, err = DecodeDataPoint(msgs[i%numPoints])
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkDecodeMetricPoint(b *testing.B) {
	numPoints := b.N
	if numPoints > 3000 {
		numPoints = 3000
	}
	msgs := generateDataPointMessages(generateMetricPoint(numPoints))
	b.ResetTimer()
	var err error
	for i := 0; i < b.N; i++ {
		_, err = DecodeDataPoint(msgs[i%numPoints])
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkDecodeMetricDataV1(b *testing.B) {
	numPoints := b.N
	if numPoints > 3000 {
		numPoints = 3000
	}
	msgs := generateMetricDataV1Message(numPoints)
	b.ResetTimer()
	var err error
	for i := 0; i < b.N; i++ {
		_, err = DecodeDataPoint(msgs[i%numPoints])
		if err != nil {
			panic(err)
		}

	}
}

func BenchmarkDecodeMetricDataV1OldStyle(b *testing.B) {
	numPoints := b.N
	if numPoints > 3000 {
		numPoints = 3000
	}
	msgs := generateMetricDataV1Message(numPoints)
	b.ResetTimer()
	var err error
	for i := 0; i < b.N; i++ {
		_, err = DecodeDecodeMetricDataV1(msgs[i%numPoints])
		if err != nil {
			panic(err)
		}
	}
}

func DecodeDecodeMetricDataV1(b []byte) (*schemaV1.MetricData, error) {
	md := new(schemaV1.MetricData)
	_, err := md.UnmarshalMsg(b)
	return md, err
}
