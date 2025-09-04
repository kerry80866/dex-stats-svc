package raft

import (
	"dex-stats-sol/internal/pkg/logger"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

var crcTable = crc32.MakeTable(crc32.Castagnoli) // Castagnoli是目前在工业界中被普遍认可为更强健的 CRC32 多项式

func crc32Checksum(buf []byte) uint32 {
	return crc32.Checksum(buf, crcTable)
}

func readFullWithLog(r io.Reader, buf []byte, content string) error {
	if len(buf) == 0 {
		return nil
	}
	n, err := io.ReadFull(r, buf)
	if err != nil {
		switch {
		case errors.Is(err, io.EOF):
			logger.Infof("[Raft] %s, reached EOF, all data read", content)
		case errors.Is(err, io.ErrUnexpectedEOF):
			logger.Errorf("[Raft] %s, truncated data: got %d bytes, want %d", content, n, len(buf))
		default:
			logger.Errorf("[Raft] %s, failed to read %d bytes: %v", content, len(buf), err)
		}
		return err
	}
	return nil
}

func errorfWithLog(format string, a ...any) error {
	logger.Errorf(format, a...)
	return fmt.Errorf("[Raft] "+format, a...)
}
