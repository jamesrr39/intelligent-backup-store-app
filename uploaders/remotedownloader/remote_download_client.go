package remotedownloader

import (
	"bytes"
	"encoding/json"
	"fmt"
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

const FilesFolderName = "files"

type FilesConfig struct {
	ModTimeKey         string            `json:"modTimeKey"`
	SizeKey            string            `json:"sizeKey"`
	RelativePathKey    string            `json:"relativePathKey"`
	FileModeKey        string            `json:"fileModeKey"`
	DownloadURLPattern string            `json:"downloadUrlPattern"`
	ForEach            []string          `json:"forEach"`
	VariableMappings   map[string]string `json:"variableMappings"`
}

type Config struct {
	Version           int
	RequiredVariables []string      `json:"requiredVariables"`
	ListingURL        string        `json:"listingUrl"`
	Files             []FilesConfig `json:"files"`
}

type downloadFileInfoType struct {
	FileInfo    *intelligentstore.FileInfo
	GetFileFunc func() (io.ReadCloser, errorsx.Error)
}

type hashesMapValueType struct {
	DownloadFiles []*intelligentstore.FileInfo
	TempFile      *dal.TempFile
}

func DownloadRemote(
	httpClient httpextra.Doer,
	storeDAL *dal.IntelligentStoreDAL,
	bucket *intelligentstore.Bucket,
	conf *Config,
	variablesKeyValues map[string]string,
) errorsx.Error {
	switch conf.Version {
	case 1:
		return downloadRemoteConfigV1(httpClient, storeDAL, bucket, conf, variablesKeyValues)
	default:
		return errorsx.Errorf("unknown config version: %d. Perhaps you need a newer version of the store program?", conf.Version)
	}
}

func downloadRemoteConfigV1(
	httpClient httpextra.Doer,
	storeDAL *dal.IntelligentStoreDAL,
	bucket *intelligentstore.Bucket,
	conf *Config,
	envVariablesKeyValues map[string]string,
) errorsx.Error {
	listingURL := makeDownloadURL(conf.ListingURL, envVariablesKeyValues)
	req, err := http.NewRequest(http.MethodGet, listingURL, nil)
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

	if resp.StatusCode != http.StatusOK {
		return errorsx.Errorf(
			"expected response code %d, but got %d from listing URL (%q). Response body:\n%s",
			http.StatusOK,
			resp.StatusCode,
			listingURL,
			bb,
		)
	}

	buf := bytes.NewReader(bb)

	fileInfosFromListing, err := getFileInfosFromListing(httpClient, conf, buf, envVariablesKeyValues)
	if err != nil {
		return errorsx.Wrap(err)
	}
	json.NewEncoder(os.Stderr).Encode(fileInfosFromListing)

	_, err = buf.Seek(0, io.SeekStart)
	if err != nil {
		return errorsx.Wrap(err)
	}
	listingFileRelativePath := intelligentstore.NewRelativePath(url.QueryEscape(listingURL))

	listingDownloadFileInfo := &downloadFileInfoType{
		FileInfo: &intelligentstore.FileInfo{
			Type:         intelligentstore.FileTypeRegular,
			RelativePath: listingFileRelativePath,
			ModTime:      time.Now(),
			Size:         int64(buf.Len()),
			FileMode:     0400,
		},
		GetFileFunc: func() (io.ReadCloser, errorsx.Error) {
			return ioutil.NopCloser(buf), nil
		},
	}

	fileInfosFromListing = append(fileInfosFromListing, listingDownloadFileInfo)

	relativePathsMap := make(map[intelligentstore.RelativePath]*downloadFileInfoType)
	for _, info := range fileInfosFromListing {
		relativePathsMap[info.FileInfo.RelativePath] = info
	}

	fileInfos := []*intelligentstore.FileInfo{}
	for _, fileInfoFromListing := range fileInfosFromListing {
		fileInfos = append(fileInfos, fileInfoFromListing.FileInfo)
	}

	// stage 1
	tx, err := storeDAL.TransactionDAL.CreateTransaction(bucket, fileInfos)
	if err != nil {
		return errorsx.Wrap(err)
	}
	defer storeDAL.TransactionDAL.Rollback(tx)

	// stage 2: calculate hashes for required relative paths
	hashesMap := make(map[intelligentstore.Hash]*hashesMapValueType)
	var relativePathAndHashesList []*intelligentstore.RelativePathWithHash

	requiredRelativePaths := tx.GetRelativePathsRequired()
	for _, relativePath := range requiredRelativePaths {
		info := relativePathsMap[relativePath]
		if info == nil {
			return errorsx.Errorf("couldn't find entry in relative path map for %q", relativePath)
		}

		relativePathWithHash, reader, err := downloadRequiredFile(info)
		if err != nil {
			return errorsx.Wrap(err)
		}
		relativePathAndHashesList = append(relativePathAndHashesList, relativePathWithHash)

		existingEntry := hashesMap[relativePathWithHash.Hash]
		if existingEntry != nil {
			existingEntry.DownloadFiles = append(existingEntry.DownloadFiles, info.FileInfo)
		} else {
			// save to temp file
			tempFile, err := storeDAL.TempStoreDAL.CreateTempFileFromReader(reader, relativePathWithHash.Hash)
			if err != nil {
				return errorsx.Wrap(err)
			}

			hashesMap[relativePathWithHash.Hash] = &hashesMapValueType{
				[]*intelligentstore.FileInfo{
					info.FileInfo,
				},
				tempFile,
			}

		}
	}

	// find out which hashes are new
	requiredHashes, err := tx.ProcessUploadHashesAndGetRequiredHashes(relativePathAndHashesList)
	if err != nil {
		return errorsx.Wrap(err)
	}

	// backup new hashes
	for _, requiredHash := range requiredHashes {
		info := hashesMap[requiredHash]

		err = storeDAL.TransactionDAL.BackupFromTempFile(tx, info.TempFile)
		if err != nil {
			return errorsx.Wrap(err)
		}
	}

	// commit
	err = storeDAL.TransactionDAL.Commit(tx)
	if err != nil {
		return errorsx.Wrap(err)
	}

	return nil
}

func getFileInfosFromListing(httpClient httpextra.Doer, conf *Config, respBuf io.Reader, envVariablesKeyValues map[string]string) ([]*downloadFileInfoType, errorsx.Error) {
	var respBody map[string]interface{}
	err := json.NewDecoder(respBuf).Decode(&respBody)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	var fileInfos []*downloadFileInfoType
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
				return nil, errorsx.Errorf("found file mode 0 on file %q", relativePath)
			}

			modTimeSecs := modTime / (1000 * 1000 * 1000)
			modTimeNanos := modTime - (modTimeSecs * 1000 * 1000 * 1000)

			if size == 0 {
				return nil, errorsx.Errorf("found file size 0 on file %q", relativePath)
			}

			variablesMap := make(map[string]string)
			for k, v := range envVariablesKeyValues {
				variablesMap[k] = v
			}

			for mappingName, mappingKey := range fileConf.VariableMappings {
				value := item[mappingKey].(string)
				variablesMap[mappingName] = value
			}

			fileInfos = append(fileInfos, &downloadFileInfoType{
				FileInfo: &intelligentstore.FileInfo{
					Type: intelligentstore.FileTypeRegular,
					// RelativePath is the relative path, with an extra folder in front to separate the listing file from data files
					RelativePath: intelligentstore.NewRelativePathFromFragments(FilesFolderName, relativePath),
					ModTime:      time.Unix(modTimeSecs, modTimeNanos),
					Size:         size,
					FileMode:     fileMode,
				},
				GetFileFunc: func() (io.ReadCloser, errorsx.Error) {
					downloadURL := makeDownloadURL(fileConf.DownloadURLPattern, variablesMap)

					req, err := http.NewRequest(http.MethodGet, downloadURL, nil)
					if err != nil {
						return nil, errorsx.Wrap(err)
					}

					resp, err := httpClient.Do(req)
					if err != nil {
						return nil, errorsx.Wrap(err)
					}

					if resp.StatusCode != http.StatusOK {
						return nil, errorsx.Errorf("expected response %d but got %d for %q (relative path: %q)", http.StatusOK, resp.StatusCode, downloadURL, relativePath)
					}

					return resp.Body, nil
				},
			})
		}
	}

	return fileInfos, nil
}

func makeDownloadURL(downloadURLPattern string, variables map[string]string) string {
	for k, v := range variables {
		downloadURLPattern = strings.Replace(downloadURLPattern, fmt.Sprintf(`${%s}`, k), v, -1)
	}
	return downloadURLPattern
}

func downloadRequiredFile(info *downloadFileInfoType) (*intelligentstore.RelativePathWithHash, *bytes.Reader, errorsx.Error) {
	var err error
	respBody, err := info.GetFileFunc()
	if err != nil {
		return nil, nil, errorsx.Wrap(err)
	}
	defer respBody.Close()

	b, err := ioutil.ReadAll(respBody)
	if err != nil {
		return nil, nil, errorsx.Wrap(err)
	}

	if len(b) != int(info.FileInfo.Size) {
		return nil, nil, errorsx.Errorf("expected size %d but got %d for %q", info.FileInfo.Size, len(b), info.FileInfo.RelativePath)
	}

	bb := bytes.NewReader(b)

	// calculate hash
	hash, err := intelligentstore.NewHash(bb)
	if err != nil {
		return nil, nil, errorsx.Wrap(err)
	}

	_, err = bb.Seek(0, io.SeekStart)
	if err != nil {
		return nil, nil, errorsx.Wrap(err)
	}

	relativePathWithHash := intelligentstore.NewRelativePathWithHash(info.FileInfo.RelativePath, hash)

	return relativePathWithHash, bb, nil
}
