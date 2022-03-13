package dal

import (
	"log"

	"github.com/jamesrr39/goutil/errorsx"
)

type MigrationFunc func(store *IntelligentStoreDAL) errorsx.Error

type Migration struct {
	Name      string
	Migration MigrationFunc
}

func GetAllMigrations() []Migration {
	return []Migration{
		{Name: "gob to json records", Migration: Run1},
		{Name: "gzip files", Migration: Run2},
		{Name: "rename revision contents with .json file extensions", Migration: Run3},
	}
}

func (s *IntelligentStoreDAL) RunMigrations(migrations []Migration) errorsx.Error {

	status, err := s.Status()
	if nil != err {
		return errorsx.Wrap(err)
	}

	for i, migration := range migrations {
		thisMigrationVersion := i + 1

		if thisMigrationVersion <= status.SchemaVersion {
			log.Printf("skipping migration version %d (schema version: %d)\n", thisMigrationVersion, status.SchemaVersion)
			continue
		}

		log.Printf("starting to run migration %d: %q\n", thisMigrationVersion, migration.Name)
		err := migration.Migration(s)
		if err != nil {
			return err
		}

		status.SchemaVersion = thisMigrationVersion

		err = s.UpdateStatus(status)
		if err != nil {
			return err
		}

		log.Printf("successfully finished running migration %d: %q\n", i+1, migration.Name)
	}

	return nil
}
