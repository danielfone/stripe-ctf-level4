package command

import (
	"github.com/goraft/raft"
	"stripe-ctf.com/sqlcluster/sql"
	"stripe-ctf.com/sqlcluster/util"
	"fmt"
  "errors"
)

// This command writes a value to a key.
type QueryCommand struct {
	Query string `json:"query"`
}

// Creates a new write command.
func NewQueryCommand(query string) *QueryCommand {
	return &QueryCommand{
		Query:   query,
	}
}

// The name of the command in the log.
func (c *QueryCommand) CommandName() string {
	return "query"
}

// Writes a value to a key.
func (c *QueryCommand) Apply(server raft.Server) (interface{}, error) {
  sql := server.Context().(*sql.SQL)
  query := c.Query
	output, err := sql.Execute("raft", query)

	if err != nil {
		var msg string
		if output != nil && len(output.Stderr) > 0 {
			template := `Error executing %#v (%s)

SQLite error: %s`
			msg = fmt.Sprintf(template, query, err.Error(), util.FmtOutput(output.Stderr))
		} else {
			msg = err.Error()
		}

		return nil, errors.New(msg)
	}

	formatted := fmt.Sprintf("SequenceNumber: %d\n%s",
		output.SequenceNumber, output.Stdout)
	return []byte(formatted), nil
}
