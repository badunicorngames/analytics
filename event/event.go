package event

import (
	"time"
)

type Event struct {
	Type      *string            `json:"type,omitempty"`
	Timestamp *time.Time         `json:"time,omitempty"`
	ClientIP  *string            `json:"clientip,omitempty"`
	Producer  *string            `json:"producer,omitempty"`
	Subject   *string            `json:"subject,omitempty"`
	Verb      *string            `json:"verb,omitempty"`
	Object    *string            `json:"object,omitempty"`
	Data      *map[string]string `json:"data,omitempty"`
}

var AvroSchema = `
{
  "type" : "record",
  "name" : "Event",
  "namespace" : "Event",
  "doc" : "A generic analytics event",
  "fields" : [
    {"name":"type", "type":"string"},
    {"name":"time", "type":"string"},
    {"name":"clientip", "type":"string"},
    {"name":"producer", "type":"string"},
    {"name":"subject", "type":"string"},
    {"name":"verb", "type":"string"},
    {"name":"object", "type":"string"},
    {"name":"data", "type": {"type":"map", "values":"string"} }
  ]
}
`
