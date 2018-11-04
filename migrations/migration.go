package migrations

type Migration func(storeLocation string) error
