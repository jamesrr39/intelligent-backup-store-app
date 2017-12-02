package storewebserver

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore"
)

type revisionInfoWithFiles struct {
	LastRevisionTs intelligentstore.RevisionVersion  `json:"revisionTs"`
	Files          []intelligentstore.FileDescriptor `json:"files"`
	Dirs           []*subDirInfo                     `json:"dirs"`
}

func (r *revisionInfoWithFiles) UnmarshalJSON(b []byte) error {
	type revInfoWithFilesIntermediateType struct {
		LastRevisionTs intelligentstore.RevisionVersion `json:"revisionTs"`
		Files          []json.RawMessage                `json:"files"`
		Dirs           []*subDirInfo                    `json:"dirs"`
	}
	revInfoIntermediate := &revInfoWithFilesIntermediateType{}

	err := json.Unmarshal(b, &revInfoIntermediate)
	if nil != err {
		return err
	}

	r.LastRevisionTs = revInfoIntermediate.LastRevisionTs
	r.Dirs = revInfoIntermediate.Dirs

	for _, rawMessage := range revInfoIntermediate.Files {
		fileInfo := intelligentstore.FileInfo{}
		err = json.Unmarshal(rawMessage, &fileInfo)
		if nil != err {
			return err
		}

		log.Printf("fileinfo: %v\n", fileInfo)

		switch fileInfo.Type {
		case intelligentstore.FileTypeRegular:
			var descriptor *intelligentstore.RegularFileDescriptor
			err = json.Unmarshal(rawMessage, &descriptor)
			if nil != err {
				return err
			}

			r.Files = append(r.Files, descriptor)

		case intelligentstore.FileTypeSymlink:
			var descriptor *intelligentstore.SymlinkFileDescriptor
			err = json.Unmarshal(rawMessage, &descriptor)
			if nil != err {
				return err
			}

			r.Files = append(r.Files, descriptor)
		default:
			return fmt.Errorf("unknown file type: %d", fileInfo.Type)
		}
	}

	return nil
}
