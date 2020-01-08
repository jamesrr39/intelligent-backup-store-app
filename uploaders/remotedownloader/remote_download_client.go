package remotedownloader

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/goutil/httpextra"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/dal"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/intelligentstore"
)

type FilesConfig struct {
	ModTimeKey         string   `json:"modTimeKey"`
	SizeKey            string   `json:"sizeKey"`
	IDKey              string   `json:"idKey"`
	RelativePathKey    string   `json:"relativePathKey"`
	FileModeKey        string   `json:"fileModeKey"`
	DownloadURLPattern string   `json:"downloadUrlPattern"`
	ForEach            []string `json:"forEach"`
}

type Config struct {
	RequiredVariables []string      `json:"requiredVariables"`
	ListingURL        string        `json:"listingUrl"`
	Files             []FilesConfig `json:"files"`
}

// {
// 	"requiredVariables": ["SERVER_BASE_URL"],
// 	"listingUrl": "${SERVER_BASE_URL}/api/graphql?query={mediaFiles{videos{relativePath,participantIds,fileSizeBytes,hashValue},pictures{relativePath,participantIds,fileSizeBytes,hashValue},tracks{relativePath,participantIds,fileSizeBytes,hashValue}},people{name,id}}",
// 	"files": [{
// 	  "immutable": true,
// 	  "urlPattern": "${SERVER_BASE_URL}/file/${VALUE}/",
//    "forEach": ["data", "videos"]
// 	  "valueFromEach: ["hashValue"]
// 	}, {
// 	  "immutable": true,
// 	  "urlPattern": "${SERVER_BASE_URL}/file/${VALUE}/",
// 	  "valueFromListing": ["mediaFiles", "pictures", "hashValue"]
// 	}, {
// 	  "immutable": true,
// 	  "urlPattern": "${SERVER_BASE_URL}/file/${VALUE}/",
// 	  "valueFromListing": ["mediaFiles", "tracks", "hashValue"]
// 	}]
//   }

func DownloadRemote(
	httpClient httpextra.Doer,
	storeDAL *dal.IntelligentStoreDAL,
	bucket *intelligentstore.Bucket,
	conf *Config,
	variablesKeyValues map[string]string,
) errorsx.Error {

	req, err := http.NewRequest(http.MethodGet, conf.ListingURL, nil)
	if err != nil {
		return errorsx.Wrap(err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return errorsx.Wrap(err)
	}
	defer resp.Body.Close()

	bb, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return errorsx.Wrap(err)
	}

	buf := bytes.NewReader(bb)

	fileInfosFromListing, err := getFileInfosFromListing(conf, buf)
	if err != nil {
		return errorsx.Wrap(err)
	}

	_, err = buf.Seek(0, io.SeekStart)
	if err != nil {
		return errorsx.Wrap(err)
	}

	fileInfos := []*intelligentstore.FileInfo{
		{
			Type:         intelligentstore.FileTypeRegular,
			RelativePath: intelligentstore.RelativePath(url.QueryEscape(conf.ListingURL)),
			ModTime:      time.Now(),
			Size:         int64(buf.Len()),
			FileMode:     0400,
		},
	}

	fileInfos = append(fileInfos, fileInfosFromListing...)

	tx, err := storeDAL.TransactionDAL.CreateTransaction(bucket, fileInfos)
	if err != nil {
		return errorsx.Wrap(err)
	}
	defer storeDAL.TransactionDAL.Rollback(tx)

	err = storeDAL.TransactionDAL.Commit(tx)
	if err != nil {
		return errorsx.Wrap(err)
	}

	return nil
}

func getFileInfosFromListing(conf *Config, respBuf io.Reader) ([]*intelligentstore.FileInfo, errorsx.Error) {
	var respBody map[string]interface{}
	err := json.NewDecoder(respBuf).Decode(&respBody)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	var fileInfos []*intelligentstore.FileInfo
	var ok bool
	for _, fileConf := range conf.Files {
		var listItems []interface{}
		obj := respBody
		for i, pathFragment := range fileConf.ForEach {
			isLastFragment := (i == len(fileConf.ForEach)-1)
			if isLastFragment {
				listItems, ok = obj[pathFragment].([]interface{})
				if !ok {
					return nil, errorsx.Errorf("couldn't process last path fragment %q, was type %T", pathFragment, obj[pathFragment])
				}
			} else {
				obj, ok = obj[pathFragment].(map[string]interface{})
				if !ok {
					return nil, errorsx.Errorf("couldn't process path fragment %q, was type %T", pathFragment, obj[pathFragment])
				}
			}
		}

		for _, itemIface := range listItems {
			item := itemIface.(map[string]interface{})
			ID := item[fileConf.IDKey].(string)
			relativePath := item[fileConf.RelativePathKey].(string)
			modTime := int64(item[fileConf.ModTimeKey].(float64))
			size := int64(item[fileConf.SizeKey].(float64))
			fileModeStr := item[fileConf.FileModeKey].(string)
			fileModeFloat, err := strconv.ParseInt(fileModeStr, 8, 64)
			if err != nil {
				return nil, errorsx.Wrap(err)
			}

			fileMode := os.FileMode(uint32(fileModeFloat))

			if fileMode == 0 {
				return nil, errorsx.Errorf("found file mode 0 on file %q", ID)
			}

			modTimeSecs := modTime / 1000
			modTimeMillis := modTime - (modTimeSecs * 1000)

			if size == 0 {
				return nil, errorsx.Errorf("found file size 0 on file %q", ID)
			}

			fileInfos = append(fileInfos, &intelligentstore.FileInfo{
				Type:         intelligentstore.FileTypeRegular,
				RelativePath: intelligentstore.RelativePath(strings.Join([]string{"data", "mediaFiles", "pictures", relativePath}, string(intelligentstore.RelativePathSep))),
				ModTime:      time.Unix(modTimeSecs, modTimeMillis*1000*1000),
				Size:         size,
				FileMode:     fileMode,
			})
		}
	}

	return fileInfos, nil
}
