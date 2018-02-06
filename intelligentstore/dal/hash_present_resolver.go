package dal

import "github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/intelligentstore"

type FsHashPresentResolver struct {
	storeDAL *IntelligentStoreDAL
}

func (r FsHashPresentResolver) IsPresent(hash intelligentstore.Hash) (bool, error) {
	return r.storeDAL.IsObjectPresent(hash)
}
