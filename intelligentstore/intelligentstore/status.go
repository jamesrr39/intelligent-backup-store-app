package intelligentstore

type Status struct {
	SchemaVersion int `json:"schemaVersion"`
}

const (
	RunMigrationsCommandName = "run-migrations"
)
