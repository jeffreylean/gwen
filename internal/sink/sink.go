package sink

import (
	"github.com/jeffreylean/gwen/internal/record"
)

type Writer interface {
	Write(records []record.Record) error
}
