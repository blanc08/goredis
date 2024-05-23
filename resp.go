package goredis

import (
	"bufio"
	"fmt"
	"io"
	"strings"
)

type messageReader struct {
	r             *bufio.Reader
	temp          []byte
	stringBuilder strings.Builder
}

func newMessageReader(r io.Reader) *messageReader {
	return &messageReader{
		r:             bufio.NewReader(r),
		temp:          make([]byte, 4096),
		stringBuilder: strings.Builder{},
	}
}

func (messageReader *messageReader) ReadArrayLength() (length int, err error) {
	b, err := messageReader.r.ReadByte()
	if err != nil {
		return 0, err
	}

	if b != '*' {
		return 0, fmt.Errorf("malformatted input")
	}

	length, err = messageReader.readValueLength()
	if err != nil {
		return 0, err
	}

	return length, nil
}

func (messageReader *messageReader) readValueLength() (int, error) {
	b, err := messageReader.r.ReadByte()
	if err != nil {
		return 0, err
	}

	if b < '0' || b > '9' {
		return 0, fmt.Errorf("malformed input")
	}
	result := int(b - '0')

	for b != '\r' {
		b, err = messageReader.r.ReadByte()
		if err != nil {
			return 0, err
		}

		if b != '\r' {
			if b < '0' || b > '9' {
				return 0, fmt.Errorf("malformed input")
			}
			result = result*10 + int(b-'0')
		}
	}

	b, err = messageReader.r.ReadByte()
	if err != nil {
		return 0, err
	}
	if b != '\n' {
		return 0, fmt.Errorf("malformed input")
	}

	return result, nil
}

func (messageReader *messageReader) readUntilCRLF(w io.Writer) error {
	b, err := messageReader.r.ReadByte()
	if err != nil {
		return err
	}

	for b != '\r' {
		if _, err := w.Write([]byte{b}); err != nil {
			return err
		}

		b, err = messageReader.r.ReadByte()
		if err != nil {
			return err
		}
	}

	b, err = messageReader.r.ReadByte()
	if err != nil {
		return err
	}
	if b != '\n' {
		return fmt.Errorf("malformed input")
	}

	return nil
}

func (messageReader *messageReader) skipCRLF() error {
	b, err := messageReader.r.ReadByte()
	if err != nil {
		return err
	}
	if b != '\r' {
		return fmt.Errorf("malformed input")
	}

	b, err = messageReader.r.ReadByte()
	if err != nil {
		return err
	}
	if b != '\n' {
		return fmt.Errorf("malformed input")
	}

	return nil
}

func (messageReader *messageReader) ReadString() (string, error) {
	b, err := messageReader.r.ReadByte()
	if err != nil {
		return "", err
	}

	if b == '$' {
		length, err := messageReader.readValueLength()
		if err != nil {
			return "", err
		}

		messageReader.stringBuilder.Reset()
		messageReader.stringBuilder.Grow(length)
		if err := messageReader.pipeToWriter(&messageReader.stringBuilder, length); err != nil {
			return "", err
		}

		s := messageReader.stringBuilder.String()
		if err := messageReader.skipCRLF(); err != nil {
			return "", err
		}

		return s, nil
	} else if b == '+' {
		messageReader.stringBuilder.Reset()
		if err := messageReader.readUntilCRLF(&messageReader.stringBuilder); err != nil {
			return "", err
		}
		return messageReader.stringBuilder.String(), nil
	} else {
		return "", fmt.Errorf("malformed input")
	}
}

func (messageReader *messageReader) pipeToWriter(w io.Writer, length int) error {
	remaining := length
	for remaining > 0 {
		n := remaining
		if n > len(messageReader.temp) {
			n = len(messageReader.temp)
		}

		if _, err := io.ReadFull(messageReader.r, messageReader.temp[:n]); err != nil {
			return err
		}
		remaining -= n

		if _, err := w.Write(messageReader.temp[:n]); err != nil {
			return err
		}
	}

	return nil
}
