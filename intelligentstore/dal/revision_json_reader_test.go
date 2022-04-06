package dal

import (
	"bytes"
	"io"
	"reflect"
	"testing"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/intelligentstore"
)

type readSeekCloserBytesReader struct {
	*bytes.Reader
}

func (readSeekCloserBytesReader) Close() error {
	return nil
}

func Test_revisionJSONReader_ReadDir(t *testing.T) {
	type fields struct {
		revisionFile io.ReadSeekCloser
	}
	type args struct {
		relativePath intelligentstore.RelativePath
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []intelligentstore.FileDescriptor
		wantErr bool
	}{
		{
			name: "root dir",
			fields: fields{
				readSeekCloserBytesReader{
					bytes.NewReader([]byte(`[{"path":"a.txt","type":1}, {"path":"dir1/b.txt","type":1}, {"path":"dir1/c.txt","type":1}]`)),
				},
			},
			args: args{
				relativePath: "",
			},
			want: []intelligentstore.FileDescriptor{
				&intelligentstore.RegularFileDescriptor{
					FileInfo: &intelligentstore.FileInfo{
						RelativePath: "a.txt",
						Type:         intelligentstore.FileTypeRegular,
					},
				},
				&intelligentstore.DirectoryFileDescriptor{
					RelativePath: "dir1",
				},
			},
		}, {
			name: "sub dir",
			fields: fields{
				readSeekCloserBytesReader{
					bytes.NewReader([]byte(`[{"path":"a.txt","type":1}, {"path":"dir1/b.txt","type":1}, {"path":"dir1/c.txt","type":1}, {"path":"dir1/dir2/d.txt","type":1}]`)),
				},
			},
			args: args{
				relativePath: "dir1",
			},
			want: []intelligentstore.FileDescriptor{
				&intelligentstore.RegularFileDescriptor{
					FileInfo: &intelligentstore.FileInfo{
						RelativePath: "dir1/b.txt",
						Type:         intelligentstore.FileTypeRegular,
					},
				},
				&intelligentstore.RegularFileDescriptor{
					FileInfo: &intelligentstore.FileInfo{
						RelativePath: "dir1/c.txt",
						Type:         intelligentstore.FileTypeRegular,
					},
				},
				&intelligentstore.DirectoryFileDescriptor{
					RelativePath: "dir1/dir2",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &revisionJSONReader{
				revisionFile: tt.fields.revisionFile,
			}
			got, err := r.ReadDir(tt.args.relativePath)
			if (err != nil) != tt.wantErr {
				t.Errorf("revisionJSONReader.ReadDir() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("revisionJSONReader.ReadDir() = %v, want %v", got, tt.want)
			}
		})
	}
}
