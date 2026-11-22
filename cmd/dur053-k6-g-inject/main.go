// dur053-k6-g-inject is the independent, one-record producer for the kind smoke.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

type receipt struct {
	Topic      string    `json:"topic"`
	Partition  int       `json:"partition"`
	Offset     int64     `json:"offset"`
	End        int64     `json:"end"`
	StartedUTC time.Time `json:"started_utc"`
	AckedUTC   time.Time `json:"acked_utc"`
}

func inject(broker, topic, value string) (receipt, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	r := receipt{Topic: topic, Partition: 0, StartedUTC: time.Now().UTC()}
	dialer := &kafka.Dialer{ClientID: "dur053-g2c2-injector"}
	conn, err := dialer.DialLeader(ctx, "tcp", broker, topic, r.Partition)
	if err != nil {
		return r, errors.New("G_INJECT_CONNECT")
	}
	defer conn.Close()
	deadline, _ := ctx.Deadline()
	if err := conn.SetDeadline(deadline); err != nil {
		return r, err
	}
	if err := conn.SetRequiredAcks(-1); err != nil {
		return r, err
	}
	before, err := conn.ReadLastOffset()
	if err != nil {
		return r, err
	}
	_, partition, offset, _, err := conn.WriteCompressedMessagesAt(nil, kafka.Message{Value: []byte(value)})
	if err != nil {
		return r, err
	}
	r.Offset = offset
	r.AckedUTC = time.Now().UTC()
	r.End, err = conn.ReadLastOffset()
	if err != nil || int(partition) != r.Partition || r.Offset != before || r.End != r.Offset+1 {
		return r, errors.New("G_INJECT_OFFSET")
	}
	return r, nil
}

func main() {
	if len(os.Args) != 4 {
		fmt.Fprintln(os.Stderr, "usage: dur053-k6-g-inject broker topic marker")
		os.Exit(2)
	}
	r, err := inject(os.Args[1], os.Args[2], os.Args[3])
	if err == nil {
		err = json.NewEncoder(os.Stdout).Encode(r)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
