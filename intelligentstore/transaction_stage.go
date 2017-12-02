package intelligentstore

type TransactionStage int

// Represents different stages in the transaction
const (
	TransactionStageAwaitingFileHashes TransactionStage = iota
	TransactionStageReadyToUploadFiles
	TransactionStageCommitted
	TransactionStageAborted
)

var transactionStages = [...]string{
	"Awaiting File Hashes",
	"Ready To Upload Files",
	"Committed",
	"Aborted",
}
