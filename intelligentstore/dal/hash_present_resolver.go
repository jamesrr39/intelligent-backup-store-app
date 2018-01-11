package dal

import "github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/domain"

type FsHashPresentResolver struct {
	storeDAL *IntelligentStoreDAL
}

func (r FsHashPresentResolver) IsPresent(hash domain.Hash) (bool, error) {
	return r.storeDAL.IsObjectPresent(hash)
}
