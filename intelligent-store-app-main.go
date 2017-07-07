package main

import (
	"fmt"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

func main() {
	initCommand := kingpin.Command("init", "create a new store")
	storeLocation := initCommand.Arg("store location", "location of the store").Default(".").String()

	initCommand.Action(func(ctx *kingpin.ParseContext) error {
		store, err := intelligentstore.CreateIntelligentStoreAndNewConn(*storeLocation)
		if nil != err {
			return err
		}

		fmt.Printf("Created a new store at '%s'\n", store.FullPathToBase)
		return nil
	})

	kingpin.Parse()

}
