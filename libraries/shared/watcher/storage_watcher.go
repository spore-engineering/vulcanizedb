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

package watcher

import (
	"database/sql"
	"fmt"
	"reflect"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	storage2 "github.com/makerdao/vulcanizedb/libraries/shared/factories/storage"
	"github.com/makerdao/vulcanizedb/libraries/shared/storage"
	"github.com/makerdao/vulcanizedb/libraries/shared/storage/types"
	"github.com/makerdao/vulcanizedb/pkg/datastore"
	"github.com/makerdao/vulcanizedb/pkg/datastore/postgres"
	"github.com/makerdao/vulcanizedb/pkg/datastore/postgres/repositories"
	"github.com/sirupsen/logrus"
)

type ErrHeaderMismatch struct {
	dbHash   string
	diffHash string
}

func NewErrHeaderMismatch(DBHash, diffHash string) *ErrHeaderMismatch {
	return &ErrHeaderMismatch{dbHash: DBHash, diffHash: diffHash}
}

func (e ErrHeaderMismatch) Error() string {
	return fmt.Sprintf("db header hash (%s) doesn't match diff header hash (%s)", e.dbHash, e.diffHash)
}

type IStorageWatcher interface {
	AddTransformers(initializers []storage2.TransformerInitializer)
	Execute() error
}

type StorageWatcher struct {
	db                        *postgres.DB
	HeaderRepository          datastore.HeaderRepository
	KeccakAddressTransformers map[common.Hash]storage2.ITransformer // keccak hash of an address => transformer
	RetryInterval             time.Duration
	StorageDiffRepository     storage.DiffRepository
}

func NewStorageWatcher(db *postgres.DB, retryInterval time.Duration) StorageWatcher {
	headerRepository := repositories.NewHeaderRepository(db)
	storageDiffRepository := storage.NewDiffRepository(db)
	transformers := make(map[common.Hash]storage2.ITransformer)
	return StorageWatcher{
		db:                        db,
		HeaderRepository:          headerRepository,
		KeccakAddressTransformers: transformers,
		RetryInterval:             retryInterval,
		StorageDiffRepository:     storageDiffRepository,
	}
}

func (watcher StorageWatcher) AddTransformers(initializers []storage2.TransformerInitializer) {
	for _, initializer := range initializers {
		storageTransformer := initializer(watcher.db)
		watcher.KeccakAddressTransformers[storageTransformer.KeccakContractAddress()] = storageTransformer
	}
}

func (watcher StorageWatcher) Execute() error {
	for {
		err := watcher.transformDiffs()
		if err != nil {
			logrus.Errorf("error transforming diffs: %s", err.Error())
			return err
		}
	}
}

func (watcher StorageWatcher) transformDiffs() error {
	diffs := make(chan types.PersistedDiff)
	errs := make(chan error)
	done := make(chan bool)

	defer close(diffs)
	defer close(errs)
	defer close(done)

	go watcher.StorageDiffRepository.GetNewDiffs(diffs, errs, done)

	for {
		select {
		case diff := <-diffs:
			err := watcher.transformDiff(diff)
			if err != nil {
				if err == sql.ErrNoRows || reflect.TypeOf(err) == reflect.TypeOf(types.ErrKeyNotFound{}) {
					logrus.Tracef("error transforming diff: %s", err.Error())
				} else {
					logrus.Infof("error transforming diff: %s", err.Error())
				}
			}
		case err := <-errs:
			return fmt.Errorf("error getting new diffs: %s", err.Error())
		case <-done:
			time.Sleep(watcher.RetryInterval)
			return nil
		}
	}
}

func (watcher StorageWatcher) transformDiff(diff types.PersistedDiff) error {
	t, watching := watcher.getTransformer(diff)
	if !watching {
		markCheckedErr := watcher.StorageDiffRepository.MarkChecked(diff.ID)
		if markCheckedErr != nil {
			return fmt.Errorf("error marking diff checked: %s", markCheckedErr.Error())
		}
		return nil
	}

	headerID, headerErr := watcher.getHeaderID(diff)
	if headerErr != nil {
		if headerErr == sql.ErrNoRows {
			return headerErr
		} else {
			return fmt.Errorf("error getting header for diff: %s", headerErr.Error())
		}
	}
	diff.HeaderID = headerID

	executeErr := t.Execute(diff)
	if executeErr != nil {
		if reflect.TypeOf(executeErr) == reflect.TypeOf(types.ErrKeyNotFound{}) {
			return executeErr
		} else {
			return fmt.Errorf("error executing storage transformer: %s", executeErr.Error())
		}
	}

	markCheckedErr := watcher.StorageDiffRepository.MarkChecked(diff.ID)
	if markCheckedErr != nil {
		return fmt.Errorf("error marking diff checked: %s", markCheckedErr.Error())
	}

	return nil
}

func (watcher StorageWatcher) getTransformer(diff types.PersistedDiff) (storage2.ITransformer, bool) {
	log.Info("EEEEEEEEEEEEEEEEEEEEEEEEEEEEE: diff's hashed address:", diff.HashedAddress.Hex())
	for hashedAddress, _ := range watcher.KeccakAddressTransformers{
		log.Info("EEEEEEEEEEEEEEEEEEEEEEEEEEEEE: Hashed address: ", hashedAddress.Hex)
	}
	storageTransformer, ok := watcher.KeccakAddressTransformers[diff.HashedAddress]
	log.Info("EEEEEEEEEEEEEEEEEEEEEEEEEEEEE: OK!!")
	return storageTransformer, ok
}

func (watcher StorageWatcher) getHeaderID(diff types.PersistedDiff) (int64, error) {
	header, getHeaderErr := watcher.HeaderRepository.GetHeader(int64(diff.BlockHeight))
	if getHeaderErr != nil {
		return 0, getHeaderErr
	}
	if diff.BlockHash != common.HexToHash(header.Hash) {
		return 0, NewErrHeaderMismatch(header.Hash, diff.BlockHash.Hex())
	}
	return header.Id, nil
}
