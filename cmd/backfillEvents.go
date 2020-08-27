package cmd

import (
	"fmt"

	"github.com/makerdao/vulcanizedb/libraries/shared/logs"
	"github.com/makerdao/vulcanizedb/utils"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var endingBlockNumber int64

// backfillEventsCmd represents the backfillEvents command
var backfillEventsCmd = &cobra.Command{
	Use:   "backfillEvents",
	Short: "BackFill events from already-checked headers",
	Long: `Fetch and persist events from configured transformers across a range
of headers that may have already been checked for logs. Useful when adding a
new event transformer to an instance that has already been running and marking
headers checked as it queried for the previous (now incomplete) set of logs.`,
	Run: func(cmd *cobra.Command, args []string) {
		SubCommand = cmd.CalledAs()
		LogWithCommand = *logrus.WithField("SubCommand", SubCommand)
		err := backFillEvents()
		if err != nil {
			logrus.Fatalf("error back-filling events: %s", err.Error())
		}
		logrus.Info("completed back-filling events")
	},
}

func init() {
	rootCmd.AddCommand(backfillEventsCmd)
	backfillEventsCmd.Flags().Int64VarP(&endingBlockNumber, "ending-block-number", "e", -1, "last block from which to back-fill events")
	backfillEventsCmd.MarkFlagRequired("ending-block-number")
}

func backFillEvents() error {
	ethEventInitializers, _, _, exportTransformersErr := exportTransformers()
	if exportTransformersErr != nil {
		LogWithCommand.Fatalf("SubCommand %v: exporting transformers failed: %v", SubCommand, exportTransformersErr)
	}

	if len(ethEventInitializers) < 1 {
		logrus.Warn("not back-filling events because no transformers configured for back-fill")
		return nil
	}

	blockChain := getBlockChain()
	db := utils.LoadPostgres(databaseConfig, blockChain.Node())

	extractor := logs.NewLogExtractor(&db, blockChain)

	for _, initializer := range ethEventInitializers {
		transformer := initializer(&db)
		err := extractor.AddTransformerConfig(transformer.GetConfig())
		if err != nil {
			return fmt.Errorf("error adding transformer: %w", err)
		}
	}

	err := extractor.BackFillLogs(endingBlockNumber)
	if err != nil {
		return fmt.Errorf("error backfilling logs: %w", err)
	}

	return nil
}
