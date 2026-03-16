package config

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

var ErrNil = errors.New("redis: nil")

type Client struct {
	addr string
}

func NewRedisClient() *Client {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	return &Client{addr: addr}
}

func (c *Client) LPush(ctx context.Context, key string, values ...string) error {
	args := make([]string, 0, len(values)+2)
	args = append(args, "LPUSH", key)
	args = append(args, values...)
	_, err := c.exec(ctx, args...)
	return err
}

func (c *Client) BRPop(ctx context.Context, timeoutSeconds int, key string) (string, error) {
	resp, err := c.exec(ctx, "BRPOP", key, strconv.Itoa(timeoutSeconds))
	if err != nil {
		return "", err
	}

	items, ok := resp.([]any)
	if !ok || len(items) != 2 {
		return "", fmt.Errorf("unexpected BRPOP response: %T", resp)
	}

	value, ok := items[1].(string)
	if !ok {
		return "", fmt.Errorf("unexpected BRPOP item type: %T", items[1])
	}

	return value, nil
}

func (c *Client) HSet(ctx context.Context, key string, fields map[string]string) error {
	args := make([]string, 0, len(fields)*2+2)
	args = append(args, "HSET", key)
	for field, value := range fields {
		args = append(args, field, value)
	}
	_, err := c.exec(ctx, args...)
	return err
}

func (c *Client) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	resp, err := c.exec(ctx, "HGETALL", key)
	if err != nil {
		return nil, err
	}

	items, ok := resp.([]any)
	if !ok {
		return nil, fmt.Errorf("unexpected HGETALL response: %T", resp)
	}

	result := make(map[string]string, len(items)/2)
	for i := 0; i+1 < len(items); i += 2 {
		field, ok := items[i].(string)
		if !ok {
			return nil, fmt.Errorf("unexpected HGETALL field type: %T", items[i])
		}
		value, ok := items[i+1].(string)
		if !ok {
			return nil, fmt.Errorf("unexpected HGETALL value type: %T", items[i+1])
		}
		result[field] = value
	}

	return result, nil
}

func (c *Client) exec(ctx context.Context, args ...string) (any, error) {
	deadline := time.Now().Add(5 * time.Second)
	if ctxDeadline, ok := ctx.Deadline(); ok {
		deadline = ctxDeadline
	}

	timeout := time.Until(deadline)
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	dialer := &net.Dialer{Timeout: timeout}
	conn, err := dialer.DialContext(ctx, "tcp", c.addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	if err := conn.SetDeadline(deadline); err != nil {
		return nil, err
	}
	if _, err := conn.Write([]byte(encodeCommand(args...))); err != nil {
		return nil, err
	}

	return decodeRESP(bufio.NewReader(conn))
}

func encodeCommand(args ...string) string {
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("*%d\r\n", len(args)))
	for _, arg := range args {
		builder.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg))
	}
	return builder.String()
}

func decodeRESP(reader *bufio.Reader) (any, error) {
	prefix, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}

	switch prefix {
	case '+':
		return readLine(reader)
	case '-':
		line, err := readLine(reader)
		if err != nil {
			return nil, err
		}
		if strings.HasPrefix(line, "nil") {
			return nil, ErrNil
		}
		return nil, errors.New(line)
	case ':':
		line, err := readLine(reader)
		if err != nil {
			return nil, err
		}
		return strconv.Atoi(line)
	case '$':
		line, err := readLine(reader)
		if err != nil {
			return nil, err
		}
		size, err := strconv.Atoi(line)
		if err != nil {
			return nil, err
		}
		if size == -1 {
			return "", ErrNil
		}
		buf := make([]byte, size+2)
		if _, err := io.ReadFull(reader, buf); err != nil {
			return nil, err
		}
		return string(buf[:size]), nil
	case '*':
		line, err := readLine(reader)
		if err != nil {
			return nil, err
		}
		count, err := strconv.Atoi(line)
		if err != nil {
			return nil, err
		}
		if count == -1 {
			return nil, ErrNil
		}
		items := make([]any, 0, count)
		for i := 0; i < count; i++ {
			item, err := decodeRESP(reader)
			if err != nil {
				return nil, err
			}
			items = append(items, item)
		}
		return items, nil
	default:
		return nil, fmt.Errorf("unexpected RESP prefix: %q", prefix)
	}
}

func readLine(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r"), nil
}
