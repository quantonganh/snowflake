package snowflake

import (
	"errors"
	"sync"
	"time"
)

const (
	datacenterIDBits = uint64(5)
	workerIDBits = uint64(5)
	sequenceBits = uint64(12)

	maxSequence = int64(-1) ^ (int64(-1) << sequenceBits)

	timestampLeftShift = uint8(datacenterIDBits + workerIDBits + sequenceBits)
	datacenterIDLeftShift = uint8(datacenterIDBits + sequenceBits)
	workerIDLeftShift = uint8(sequenceBits)

	// Fri Dec 10 15:41:48 +07 2021
	epoch = int64(1639125652077)
)

type Worker struct {
	mu            sync.Mutex
	lastTimestamp int64
	datacenterID  int64
	workerID int64
	sequence int64
}

func NewWorker(datacenterID, workerID int64) *Worker {
	return &Worker{
		datacenterID: datacenterID,
		workerID:     workerID,
	}
}

func (w *Worker) NextID() (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.nextID()
}

func (w *Worker) nextID() (uint64, error) {
	timestamp := getMilliseconds()
	if timestamp < w.lastTimestamp {
		return 0, errors.New("time drift")
	}

	if w.lastTimestamp == timestamp {
		w.sequence = (w.sequence + 1) & maxSequence
		if w.sequence == 0 {
			for timestamp <= w.lastTimestamp {
				timestamp = getMilliseconds()
			}
		}
	}

	w.lastTimestamp = timestamp
	id := ((timestamp - epoch) << timestampLeftShift) |
		(w.datacenterID << datacenterIDLeftShift) |
		(w.workerID << workerIDLeftShift) |
		w.sequence
	return uint64(id), nil
}

func getMilliseconds() int64 {
	return time.Now().UnixNano() / 1e6
}