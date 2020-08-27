// VulcanizeDB
// Copyright © 2019 Vulcanize

// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.

// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"sync"
	"time"

	"github.com/makerdao/vulcanizedb/libraries/shared/constants"
	"github.com/makerdao/vulcanizedb/libraries/shared/factories/event"
	"github.com/makerdao/vulcanizedb/libraries/shared/factories/storage"
	"github.com/makerdao/vulcanizedb/libraries/shared/logs"
	"github.com/makerdao/vulcanizedb/libraries/shared/transformer"
	"github.com/makerdao/vulcanizedb/libraries/shared/watcher"
	"github.com/makerdao/vulcanizedb/pkg/fs"
	"github.com/makerdao/vulcanizedb/utils"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// executeCmd represents the execute command
var executeCmd = &cobra.Command{
	Use:   "execute",
	Short: "executes a precomposed transformer initializer plugin",
	Long: `This command needs a config .toml file of form:

[database]
    name     = "vulcanize_public"
    hostname = "localhost"
    user     = "vulcanize"
    password = "vulcanize"
    port     = 5432

[client]
    ipcPath  = "/Users/user/Library/Ethereum/geth.ipc"

[exporter]
    name     = "exampleTransformerExporter"

Note: If any of the plugin transformer need additional
configuration variables include them in the .toml file as well

The exporter.name is the name (without extension) of the plugin to be loaded.
The plugin file needs to be located in the /plugins directory and this command assumes 
the db migrations remain from when the plugin was composed. Additionally, the plugin 
must have been composed by the same version of vulcanizedb or else it will not be compatible.

Specify config location when executing the command:
./vulcanizedb execute --config=./environments/config_name.toml`,
	Run: func(cmd *cobra.Command, args []string) {
		SubCommand = cmd.CalledAs()
		LogWithCommand = *logrus.WithField("SubCommand", SubCommand)
		execute()
	},
}

func execute() {
	executeTransformers()
}

func init() {
	rootCmd.AddCommand(executeCmd)
	executeCmd.Flags().BoolVarP(&recheckHeadersArg, "recheck-headers", "r", false, "whether to re-check headers for watched events")
	executeCmd.Flags().DurationVarP(&retryInterval, "retry-interval", "i", 7*time.Second, "interval duration between retries on execution error")
	executeCmd.Flags().IntVarP(&maxUnexpectedErrors, "max-unexpected-errs", "m", 5, "maximum number of unexpected errors to allow (with retries) before exiting")
	executeCmd.Flags().Int64VarP(&diffBlockFromHeadOfChain, "diff-blocks-from-head", "d", -1, "number of blocks from head of chain to start reprocessing diffs, defaults to -1 so all diffs are processsed")
}

func executeTransformers() {
	ethEventInitializers, ethStorageInitializers, ethContractInitializers, exportTransformersErr := exportTransformers()
	if exportTransformersErr != nil {
		LogWithCommand.Fatalf("SubCommand %v: exporting transformers failed: %v", SubCommand, exportTransformersErr)
	}

	// Setup bc and db objects
	blockChain := getBlockChain()
	db := utils.LoadPostgres(databaseConfig, blockChain.Node())
	healthCheckFile := "/tmp/execute_health_check"

	// Execute over transformer sets returned by the exporter
	// Use WaitGroup to wait on both goroutines
	var wg sync.WaitGroup
	if len(ethEventInitializers) > 0 {
		extractor := logs.NewLogExtractor(&db, blockChain)
		delegator := logs.NewLogDelegator(&db)
		eventHealthCheckMessage := []byte("event watcher starting\n")
		statusWriter := fs.NewStatusWriter(healthCheckFile, eventHealthCheckMessage)
		ew := watcher.NewEventWatcher(&db, blockChain, extractor, delegator, maxUnexpectedErrors, retryInterval, statusWriter)
		addErr := ew.AddTransformers(ethEventInitializers)
		if addErr != nil {
			LogWithCommand.Fatalf("failed to add event transformer initializers to watcher: %s", addErr.Error())
		}
		wg.Add(1)
		go watchEthEvents(&ew, &wg)
	}

	if len(ethStorageInitializers) > 0 {
		storageHealthCheckMessage := []byte("storage watcher for new diffs starting\n")
		statusWriter := fs.NewStatusWriter(healthCheckFile, storageHealthCheckMessage)
		sw := watcher.NewStorageWatcher(&db, diffBlockFromHeadOfChain, statusWriter, watcher.New)
		sw.AddTransformers(ethStorageInitializers)
		wg.Add(1)
		go watchEthStorage(&sw, &wg)
	}

	if len(ethStorageInitializers) > 0 {
		storageHealthCheckMessage := []byte("storage watcher for unrecognized diffs starting\n")
		statusWriter := fs.NewStatusWriter(healthCheckFile, storageHealthCheckMessage)
		sw := watcher.NewStorageWatcher(&db, diffBlockFromHeadOfChain, statusWriter, watcher.Unrecognized)
		sw.AddTransformers(ethStorageInitializers)
		wg.Add(1)
		go watchEthStorage(&sw, &wg)
	}

	if len(ethContractInitializers) > 0 {
		gw := watcher.NewContractWatcher(&db, blockChain)
		gw.AddTransformers(ethContractInitializers)
		wg.Add(1)
		go watchEthContract(&gw, &wg)
	}
	wg.Wait()
}

type Exporter interface {
	Export() ([]event.TransformerInitializer, []storage.TransformerInitializer, []transformer.ContractTransformerInitializer)
}

func watchEthEvents(w *watcher.EventWatcher, wg *sync.WaitGroup) {
	defer wg.Done()
	// Execute over the EventTransformerInitializer set using the watcher
	LogWithCommand.Info("executing event transformers")
	var recheck constants.TransformerExecution
	if recheckHeadersArg {
		recheck = constants.HeaderRecheck
	} else {
		recheck = constants.HeaderUnchecked
	}
	err := w.Execute(recheck)
	if err != nil {
		LogWithCommand.Fatalf("error executing event watcher: %s", err.Error())
	}
}

func watchEthStorage(w watcher.IStorageWatcher, wg *sync.WaitGroup) {
	defer wg.Done()
	// Execute over the storage.TransformerInitializer set using the storage watcher
	LogWithCommand.Info("executing storage transformers")
	err := w.Execute()
	if err != nil {
		LogWithCommand.Fatalf("error executing storage watcher: %s", err.Error())
	}
}

func watchEthContract(w *watcher.ContractWatcher, wg *sync.WaitGroup) {
	defer wg.Done()
	// Execute over the ContractTransformerInitializer set using the contract watcher
	LogWithCommand.Info("executing contract_watcher transformers")
	ticker := time.NewTicker(pollingInterval)
	defer ticker.Stop()
	for range ticker.C {
		err := w.Execute()
		if err != nil {
			LogWithCommand.Errorf("error executing contract watcher: %s", err.Error())
		}
	}
}
