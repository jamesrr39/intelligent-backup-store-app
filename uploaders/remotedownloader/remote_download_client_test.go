package remotedownloader

import (
	"bytes"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/intelligentstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_getFileInfosFromListing(t *testing.T) {
	type args struct {
		conf     *Config
		respBody io.Reader
	}
	tests := []struct {
		name  string
		args  args
		want  []*intelligentstore.FileInfo
		want1 errorsx.Error
	}{
		{
			name: "",
			args: args{
				conf: &Config{
					Files: []FilesConfig{
						{
							ModTimeKey:      "modTime",
							FileModeKey:     "fileMode",
							SizeKey:         "fileSizeBytes",
							IDKey:           "hashValue",
							RelativePathKey: "hashValue",
							ForEach:         []string{"data", "mediaFiles", "pictures"},
						},
					},
				},
				respBody: bytes.NewBufferString(`{
					"data":{
						"mediaFiles":{
							"pictures":[
								{
									"modTime": 0,
									"fileSizeBytes": 1000,
									"hashValue": "abcdef123456",
									"fileMode": "0600"
								},
								{
									"modTime": 1001,
									"fileSizeBytes": 2500,
									"hashValue": "xyz123",
									"fileMode": "700"
								}
							]
						}
					}
				}`),
			},
			want: []*intelligentstore.FileInfo{
				{
					Type:         intelligentstore.FileTypeRegular,
					RelativePath: intelligentstore.RelativePath(strings.Join([]string{"data", "mediaFiles", "pictures", "abcdef123456"}, string(intelligentstore.RelativePathSep))),
					ModTime:      time.Unix(0, 0),
					Size:         1000,
					FileMode:     0600,
				},
				{
					Type:         intelligentstore.FileTypeRegular,
					RelativePath: intelligentstore.RelativePath(strings.Join([]string{"data", "mediaFiles", "pictures", "xyz123"}, string(intelligentstore.RelativePathSep))),
					ModTime:      time.Unix(1, 1*1000*1000),
					Size:         2500,
					FileMode:     0700,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := getFileInfosFromListing(tt.args.conf, tt.args.respBody)
			require.Equal(t, tt.want1, got1)
			assert.Equal(t, tt.want, got)
		})
	}
}
