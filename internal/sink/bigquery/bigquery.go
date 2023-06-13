package bigquery

import (
	"context"
	"errors"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	"github.com/jeffreylean/gwen/internal/record"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

type Writer struct {
	dataset           string
	table             *bigquery.Table
	client            *bigquery.Client
	managedClient     *managedwriter.Client
	managedStream     *managedwriter.ManagedStream
	messageDescriptor *protoreflect.MessageDescriptor
	context           context.Context
}

// New creates a new writer.
func New(project, dataset, table string) (*Writer, error) {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, project)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("bigquery: %v", err))
	}
	managedClient, err := managedwriter.NewClient(ctx, project)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("bigquery: %v", err))
	}

	tableRef := client.Dataset(dataset).Table(table)
	inserter := tableRef.Inserter()
	inserter.SkipInvalidRows = true
	inserter.IgnoreUnknownValues = true

	mt, err := tableRef.Metadata(ctx)
	if err != nil {
		return nil, err
	}
	md, descriptorProto, err := setupDynamicDescriptors(mt.Schema)

	ms, err := managedClient.NewManagedStream(ctx,
		managedwriter.WithDestinationTable(managedwriter.TableParentFromParts(tableRef.ProjectID, tableRef.DatasetID, tableRef.TableID)),
		managedwriter.WithType(managedwriter.DefaultStream),
		managedwriter.WithSchemaDescriptor(descriptorProto),
	)
	if err != nil {
		return nil, err
	}

	w := &Writer{
		dataset:           dataset,
		table:             tableRef,
		managedClient:     managedClient,
		managedStream:     ms,
		messageDescriptor: &md,
		context:           ctx,
	}
	return w, nil
}

func (w *Writer) Write(records []record.Record) error {
	for _, record := range records {
		message := dynamicpb.NewMessage(*w.messageDescriptor)
		if err := protojson.Unmarshal(record, message); err != nil {
			return err
		}
		b, err := proto.Marshal(message)
		if err != nil {
			return err
		}

		_, err = w.managedStream.AppendRows(w.context, [][]byte{b})
		if err != nil {
			return err
		}
	}
	return nil
}

// SetupDynamicDescriptors aids testing when not using a supplied proto
func setupDynamicDescriptors(schema bigquery.Schema) (protoreflect.MessageDescriptor, *descriptorpb.DescriptorProto, error) {
	convertedSchema, err := adapt.BQSchemaToStorageTableSchema(schema)
	if err != nil {
		return nil, nil, err
	}

	descriptor, err := adapt.StorageSchemaToProto2Descriptor(convertedSchema, "root")
	if err != nil {
		return nil, nil, err
	}
	messageDescriptor, ok := descriptor.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, nil, errors.New("adapted descriptor is not a message descriptor")
	}
	return messageDescriptor, protodesc.ToDescriptorProto(messageDescriptor), nil
}

// Mapping some field to bigquery supported field
func mapFieldToBQ(i interface{}) interface{} {
	switch i.(type) {
	case time.Time:
		// Bigquery managedStream api did not support time.Time, need to convert to unixmicro int64
		return i.(time.Time).UnixMicro()
	default:
		return i
	}
}
