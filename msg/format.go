package msg

//go:generate stringer -type=Format

type Format uint8

// identifier of message format when ingesting over HTTP
const (
	FormatMetricDataV1ArrayJson Format = iota
	FormatMetricDataV1ArrayMsgp

	FormatMetricDataArrayJson
	FormatMetricDataArrayMsgp
)

// identifier of message format when ingesting over Kafka
const (
	FormatMetricData Format = iota
	FormatMetricPoint
)
