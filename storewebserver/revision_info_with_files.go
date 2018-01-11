package storewebserver

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/domain"
)

func (r *revisionInfoWithFiles) UnmarshalJSON(b []byte) error {
	type revInfoWithFilesIntermediateType struct {
		LastRevisionTs domain.RevisionVersion `json:"revisionTs"`
		Files          []json.RawMessage      `json:"files"`
		Dirs           []*subDirInfo          `json:"dirs"`
	}
	revInfoIntermediate := &revInfoWithFilesIntermediateType{}

	err := json.Unmarshal(b, &revInfoIntermediate)
	if nil != err {
		return err
	}

	r.LastRevisionTs = revInfoIntermediate.LastRevisionTs
	r.Dirs = revInfoIntermediate.Dirs

	for _, rawMessage := range revInfoIntermediate.Files {
		fileInfo := domain.FileInfo{}
		err = json.Unmarshal(rawMessage, &fileInfo)
		if nil != err {
			return err
		}

		log.Printf("fileinfo: %v\n", fileInfo)

		switch fileInfo.Type {
		case domain.FileTypeRegular:
			var descriptor *domain.RegularFileDescriptor
			err = json.Unmarshal(rawMessage, &descriptor)
			if nil != err {
				return err
			}

			r.Files = append(r.Files, descriptor)

		case domain.FileTypeSymlink:
			var descriptor *domain.SymlinkFileDescriptor
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
