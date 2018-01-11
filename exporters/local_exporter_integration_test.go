// +build integration

package exporters

import (
	"io/ioutil"
	"path/filepath"
	"testing"
	"time"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/domain"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/storetest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_LocalExporterIntegration(t *testing.T) {
	store := storetest.CreateOsFsTestStore(t)
	defer store.Close(t)

	bucket := storetest.CreateBucket(t, store.Store, "docs")

	regularFile := domain.NewRegularFileDescriptorWithContents(t, "a.txt", time.Unix(0, 0), 0700, []byte("file a contents"))
	storetest.CreateRevision(t, store.Store, bucket, []*domain.RegularFileDescriptorWithContents{regularFile})

	exporter := NewLocalExporter(store.Store, bucket.BucketName, store.ExportDir, nil, nil)

	err := exporter.Export()
	require.Nil(t, err)

	contents, err := ioutil.ReadFile(filepath.Join(exporter.ExportDir, FilesExportSubDir, "a.txt"))
	require.Nil(t, err)

	assert.Equal(t, regularFile.Contents, contents)
}
