package queue

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/ayvazov-i/library-prep-pipeline/internal/platform"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
)

const (
	TasksStream      = "TASKS"
	EventsStream     = "PLATFORM_EVENTS"
	DLQStream        = "TASK_DLQ"
	AdvisoriesStream = "TASK_ADVISORIES"
)

type Client struct {
	nc  *nats.Conn
	js  nats.JetStreamContext
	log *slog.Logger
}

func Connect(url, name string, tlsConfig *tls.Config, log *slog.Logger) (*Client, error) {
	options := []nats.Option{
		nats.Name(name), nats.Timeout(10 * time.Second), nats.PingInterval(20 * time.Second),
		nats.MaxPingsOutstanding(3), nats.ReconnectWait(2 * time.Second), nats.MaxReconnects(-1),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) { log.Warn("NATS disconnected", "error", err) }),
		nats.ReconnectHandler(func(conn *nats.Conn) { log.Info("NATS reconnected", "url", conn.ConnectedUrl()) }),
	}
	if tlsConfig != nil {
		options = append(options, nats.Secure(tlsConfig))
	}
	nc, err := nats.Connect(url, options...)
	if err != nil {
		return nil, err
	}
	js, err := nc.JetStream(nats.PublishAsyncMaxPending(256))
	if err != nil {
		nc.Close()
		return nil, err
	}
	return &Client{nc: nc, js: js, log: log}, nil
}

func (c *Client) Close() {
	_ = c.nc.Drain()
	c.nc.Close()
}

func (c *Client) Ready() error {
	if !c.nc.IsConnected() {
		return errors.New("NATS is not connected")
	}
	return nil
}

func (c *Client) EnsureStreams() error {
	streams := []*nats.StreamConfig{
		{Name: TasksStream, Subjects: []string{"tasks.>"}, Retention: nats.WorkQueuePolicy, Storage: nats.FileStorage, Replicas: 1, MaxAge: 7 * 24 * time.Hour, Duplicates: 24 * time.Hour},
		{Name: EventsStream, Subjects: []string{"events.>"}, Retention: nats.LimitsPolicy, Storage: nats.FileStorage, Replicas: 1, MaxAge: 7 * 24 * time.Hour, Duplicates: 24 * time.Hour},
		{Name: DLQStream, Subjects: []string{"dlq.>"}, Retention: nats.LimitsPolicy, Storage: nats.FileStorage, Replicas: 1, MaxAge: 30 * 24 * time.Hour, Duplicates: 24 * time.Hour},
		{Name: AdvisoriesStream, Subjects: []string{"$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.>"}, Retention: nats.LimitsPolicy, Storage: nats.FileStorage, Replicas: 1, MaxAge: 30 * 24 * time.Hour, Duplicates: 24 * time.Hour},
	}
	for _, config := range streams {
		info, err := c.js.StreamInfo(config.Name)
		if errors.Is(err, nats.ErrStreamNotFound) {
			if _, err = c.js.AddStream(config); err != nil {
				return fmt.Errorf("create stream %s: %w", config.Name, err)
			}
			continue
		}
		if err != nil {
			return err
		}
		config.MaxBytes = info.Config.MaxBytes
		if _, err = c.js.UpdateStream(config); err != nil {
			return fmt.Errorf("update stream %s: %w", config.Name, err)
		}
	}
	for _, capability := range []string{"cpu", "gpu"} {
		durable := "workers_" + capability
		config := &nats.ConsumerConfig{
			Durable: durable, AckPolicy: nats.AckExplicitPolicy, FilterSubject: "tasks." + capability,
			AckWait: 2 * time.Minute, MaxDeliver: 4,
			// The first unacknowledged redelivery must occur after the 90-second
			// PostgreSQL lease can be reaped. Application-classified retries use
			// NakWithDelay and retain their 30s/2m/10m policy.
			BackOff:       []time.Duration{2 * time.Minute, 2 * time.Minute, 10 * time.Minute},
			MaxAckPending: 64, ReplayPolicy: nats.ReplayInstantPolicy,
		}
		if _, err := c.js.ConsumerInfo(TasksStream, durable); errors.Is(err, nats.ErrConsumerNotFound) {
			if _, err = c.js.AddConsumer(TasksStream, config); err != nil {
				return fmt.Errorf("create consumer %s: %w", durable, err)
			}
		} else if err != nil {
			return fmt.Errorf("inspect consumer %s: %w", durable, err)
		} else if _, err = c.js.UpdateConsumer(TasksStream, config); err != nil {
			return fmt.Errorf("update consumer %s: %w", durable, err)
		}
	}
	return nil
}

func (c *Client) PublishOutbox(_ context.Context, event platform.OutboxEvent) error {
	msg := &nats.Msg{Subject: event.Subject, Data: event.Payload, Header: nats.Header{}}
	msg.Header.Set(nats.MsgIdHdr, event.ID.String())
	msg.Header.Set("X-Event-Type", event.EventType)
	msg.Header.Set("X-Aggregate-ID", event.AggregateID.String())
	_, err := c.js.PublishMsg(msg)
	return err
}

type maxDeliveriesAdvisory struct {
	Stream     string `json:"stream"`
	Consumer   string `json:"consumer"`
	StreamSeq  uint64 `json:"stream_seq"`
	Deliveries uint64 `json:"deliveries"`
}

func (c *Client) SubscribeMaxDeliveries(ctx context.Context, markFailed func(context.Context, uuid.UUID) error) (*nats.Subscription, error) {
	return c.js.Subscribe("$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.>", func(advisoryMessage *nats.Msg) {
		var advisory maxDeliveriesAdvisory
		if err := json.Unmarshal(advisoryMessage.Data, &advisory); err != nil {
			c.log.Error("invalid max-deliveries advisory", "error", err)
			_ = advisoryMessage.Term()
			return
		}
		raw, err := c.js.GetMsg(advisory.Stream, advisory.StreamSeq)
		if err != nil {
			if errors.Is(err, nats.ErrMsgNotFound) {
				_ = advisoryMessage.Ack()
				return
			}
			c.log.Error("retrieve exhausted message", "stream", advisory.Stream, "sequence", advisory.StreamSeq, "error", err)
			_ = advisoryMessage.NakWithDelay(30 * time.Second)
			return
		}
		var reference struct {
			TaskID uuid.UUID `json:"task_id"`
		}
		if err = json.Unmarshal(raw.Data, &reference); err != nil || reference.TaskID == uuid.Nil {
			c.log.Error("exhausted message has no task ID", "stream", advisory.Stream, "sequence", advisory.StreamSeq)
			_ = advisoryMessage.Term()
			return
		}
		dlq := &nats.Msg{Subject: "dlq.tasks.max_deliveries", Data: raw.Data, Header: nats.Header{}}
		dlq.Header.Set(nats.MsgIdHdr, fmt.Sprintf("%s-%d", advisory.Stream, advisory.StreamSeq))
		dlq.Header.Set("X-Source-Stream", advisory.Stream)
		dlq.Header.Set("X-Source-Sequence", fmt.Sprint(advisory.StreamSeq))
		dlq.Header.Set("X-Consumer", advisory.Consumer)
		dlq.Header.Set("X-Deliveries", fmt.Sprint(advisory.Deliveries))
		if _, err = c.js.PublishMsg(dlq); err != nil {
			c.log.Error("publish exhausted task to DLQ", "task_id", reference.TaskID, "error", err)
			_ = advisoryMessage.NakWithDelay(30 * time.Second)
			return
		}
		if err = markFailed(ctx, reference.TaskID); err != nil {
			c.log.Error("mark exhausted task failed", "task_id", reference.TaskID, "error", err)
			_ = advisoryMessage.NakWithDelay(30 * time.Second)
			return
		}
		if err = c.js.DeleteMsg(advisory.Stream, advisory.StreamSeq); err != nil {
			c.log.Error("delete exhausted source message", "stream", advisory.Stream, "sequence", advisory.StreamSeq, "error", err)
			_ = advisoryMessage.NakWithDelay(30 * time.Second)
			return
		}
		_ = advisoryMessage.Ack()
	}, nats.BindStream(AdvisoriesStream), nats.Durable("max_deliveries_reconciler"), nats.ManualAck(), nats.AckExplicit(), nats.DeliverAll())
}

func (c *Client) Consumer(subject, durable string) (*nats.Subscription, error) {
	return c.js.PullSubscribe(subject, durable, nats.Bind(TasksStream, durable))
}
