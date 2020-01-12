package remotedownloader

import (
	"bytes"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/goutil/httpextra"
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
		want  []*downloadFileInfoType
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
							RelativePathKey: "relativePath",
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
									"fileMode": "0600",
									"relativePath": "dir1/file1.txt"
								},
								{
									"modTime": 1001000000,
									"fileSizeBytes": 2500,
									"hashValue": "xyz123",
									"fileMode": "700",
									"relativePath": "dir1/file2.txt"
								}
							]
						}
					}
				}`),
			},
			want: []*downloadFileInfoType{
				{
					FileInfo: &intelligentstore.FileInfo{
						Type:         intelligentstore.FileTypeRegular,
						RelativePath: intelligentstore.RelativePath(strings.Join([]string{FilesFolderName, "dir1", "file1.txt"}, string(intelligentstore.RelativePathSep))),
						ModTime:      time.Unix(0, 0),
						Size:         1000,
						FileMode:     0600,
					},
				}, {
					FileInfo: &intelligentstore.FileInfo{
						Type:         intelligentstore.FileTypeRegular,
						RelativePath: intelligentstore.RelativePath(strings.Join([]string{FilesFolderName, "dir1", "file2.txt"}, string(intelligentstore.RelativePathSep))),
						ModTime:      time.Unix(1, 1*1000*1000),
						Size:         2500,
						FileMode:     0700,
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			httpClient := &httpextra.MockDoer{}
			downloadInfos, err := getFileInfosFromListing(httpClient, tt.args.conf, tt.args.respBody, nil)
			require.Equal(t, tt.want1, err)
			require.Len(t, downloadInfos, len(tt.want))
			for i, downloadInfo := range downloadInfos {
				assert.Equal(t, tt.want[i].FileInfo, downloadInfo.FileInfo)
			}
		})
	}
}
