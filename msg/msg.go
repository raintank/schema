package msg

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"

	schemaV1 "gopkg.in/raintank/schema.v1"
	schema "gopkg.in/raintank/schema.v2"
)

var errTooSmall = errors.New("too small")
var errFmtBinWriteFailed = "binary write failed: %q"
var errFmtUnknownFormat = "unknown format %d"

func EncodeMetricDataArray(metrics []*schema.MetricData, version Format) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, uint8(version))
	if err != nil {
		return nil, fmt.Errorf(errFmtBinWriteFailed, err)
	}
	var msg []byte
	switch version {
	case FormatMetricDataArrayJson:
		msg, err = json.Marshal(metrics)
	case FormatMetricDataArrayMsgp:
		m := schema.MetricDataArray(metrics)
		msg, err = m.MarshalMsg(nil)
	default:
		return nil, fmt.Errorf(errFmtUnknownFormat, version)
	}
	if err != nil {
		return nil, fmt.Errorf("Failed to marshal metrics payload: %s", err)
	}
	_, err = buf.Write(msg)
	if err != nil {
		return nil, fmt.Errorf(errFmtBinWriteFailed, err)
	}
	return buf.Bytes(), nil
}

func DecodeMetricDataArray(b []byte) ([]*schema.MetricData, error) {
	// We need at least the format field.
	if len(b) < 1 {
		return nil, errTooSmall
	}
	metrics := make([]*schema.MetricData, 0)
	format := Format(b[0])
	switch format {
	case FormatMetricDataArrayJson:
		err := json.Unmarshal(b[1:], &metrics)
		if err != nil {
			return nil, fmt.Errorf("ERROR: failure to unmarshal message body via format %q: %s", format, err)
		}
	case FormatMetricDataArrayMsgp:
		out := schema.MetricDataArray(metrics)
		_, err := out.UnmarshalMsg(b[1:])
		if err != nil {
			return nil, fmt.Errorf("ERROR: failure to unmarshal message body via format %q: %s", format, err)
		}
		metrics = []*schema.MetricData(out)
	case FormatMetricDataV1ArrayJson:
		// format + ID field is 9bytes.
		if len(b) < 9 {
			return nil, errTooSmall
		}
		oldMetrics := make([]*schemaV1.MetricData, 0)
		err := json.Unmarshal(b[9:], &oldMetrics)
		if err != nil {
			return nil, fmt.Errorf("ERROR: failure to unmarshal message body via format %q: %s", format, err)
		}
		metrics = make([]*schema.MetricData, len(oldMetrics))
		for i, om := range oldMetrics {
			metrics[i] = &schema.MetricData{
				MetricMetadata: schema.MetricMetadata{
					Id:    om.Id,
					OrgId: om.OrgId,
					Name:  om.Name,
					Unit:  om.Unit,
					Mtype: om.Mtype,
					Tags:  om.Tags,
				},
				Point: schema.Point{
					Ts:  uint32(om.Time),
					Val: om.Value,
				},
			}
		}
	case FormatMetricDataV1ArrayMsgp:
		// format + ID field is 9bytes.
		if len(b) < 9 {
			return nil, errTooSmall
		}
		oldMetrics := make([]*schemaV1.MetricData, 0)
		out := schemaV1.MetricDataArray(oldMetrics)
		_, err := out.UnmarshalMsg(b[9:])
		if err != nil {
			return nil, fmt.Errorf("ERROR: failure to unmarshal message body via format %q: %s", format, err)
		}
		oldMetrics = []*schemaV1.MetricData(out)
		for i, om := range oldMetrics {
			metrics[i] = &schema.MetricData{
				MetricMetadata: schema.MetricMetadata{
					Id:    om.Id,
					OrgId: om.OrgId,
					Name:  om.Name,
					Unit:  om.Unit,
					Mtype: om.Mtype,
					Tags:  om.Tags,
				},
				Point: schema.Point{
					Ts:  uint32(om.Time),
					Val: om.Value,
				},
			}
		}
	default:
		return nil, fmt.Errorf(errFmtUnknownFormat, format)
	}
	return metrics, nil
}

// Marshal a DataPoint to byte stream
func EncodeDataPoint(p schema.DataPoint, b []byte) ([]byte, error) {
	buf := bytes.NewBuffer(b)
	switch p.(type) {
	case *schema.MetricData:
		err := binary.Write(buf, binary.LittleEndian, uint8(FormatMetricData))
		if err != nil {
			return nil, fmt.Errorf(errFmtBinWriteFailed, err)
		}
		msg, err := p.(*schema.MetricData).MarshalMsg(nil)
		if err != nil {
			return nil, fmt.Errorf(errFmtBinWriteFailed, err)
		}
		buf.Write(msg)
	case *schema.MetricPoint:
		err := binary.Write(buf, binary.LittleEndian, uint8(FormatMetricPoint))
		if err != nil {
			return nil, fmt.Errorf(errFmtBinWriteFailed, err)
		}
		msg, err := p.(*schema.MetricPoint).MarshalMsg(nil)
		if err != nil {
			return nil, fmt.Errorf(errFmtBinWriteFailed, err)
		}
		buf.Write(msg)
	default:
		return nil, fmt.Errorf("unknown DataPoint type")
	}

	return buf.Bytes(), nil
}

func DecodeDataPoint(b []byte) (schema.DataPoint, error) {
	switch Format(b[0]) {
	case FormatMetricPoint:
		p := new(schema.MetricPoint)
		_, err := p.UnmarshalMsg(b[1:])
		if err != nil {
			return nil, err
		}
		return p, nil
	case FormatMetricData:
		p := new(schema.MetricData)
		_, err := p.UnmarshalMsg(b[1:])
		if err != nil {
			return nil, err
		}
		return p, nil
	default:
		// if format is not known, we have to assume that this is an old MetricData message with no version byte
		om := schemaV1.MetricData{}
		_, err := om.UnmarshalMsg(b)
		if err != nil {
			return nil, err
		}
		p := &schema.MetricData{
			MetricMetadata: schema.MetricMetadata{
				Id:    om.Id,
				OrgId: om.OrgId,
				Name:  om.Name,
				Unit:  om.Unit,
				Mtype: om.Mtype,
				Tags:  om.Tags,
			},
			Point: schema.Point{
				Ts:  uint32(om.Time),
				Val: om.Value,
			},
		}
		return p, nil
	}

	// this will never be reached
	return nil, fmt.Errorf(errFmtUnknownFormat, Format(b[0]))
}
