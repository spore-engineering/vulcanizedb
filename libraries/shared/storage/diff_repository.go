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

package storage

import (
	"fmt"

	"github.com/makerdao/vulcanizedb/libraries/shared/storage/types"
	"github.com/makerdao/vulcanizedb/pkg/datastore/postgres"
)

type DiffRepository interface {
	CreateStorageDiff(rawDiff types.RawDiff) (int64, error)
	CreateBackFilledStorageValue(rawDiff types.RawDiff) error
	GetNewDiffs(minID, limit int) ([]types.PersistedDiff, error)
	MarkChecked(id int64) error
	GetFirstDiffIDForBlockHeight(blockHeight int64) (int64, error)
}

type diffRepository struct {
	db *postgres.DB
}

func NewDiffRepository(db *postgres.DB) diffRepository {
	return diffRepository{db: db}
}

// CreateStorageDiff writes a raw storage diff to the database
func (repository diffRepository) CreateStorageDiff(rawDiff types.RawDiff) (int64, error) {
	var storageDiffID int64
	row := repository.db.QueryRowx(`INSERT INTO public.storage_diff
		(hashed_address, block_height, block_hash, storage_key, storage_value, eth_node_id) VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT DO NOTHING RETURNING id`, rawDiff.HashedAddress.Bytes(), rawDiff.BlockHeight, rawDiff.BlockHash.Bytes(),
		rawDiff.StorageKey.Bytes(), rawDiff.StorageValue.Bytes(), repository.db.NodeID)
	err := row.Scan(&storageDiffID)
	if err != nil {
		return 0, fmt.Errorf("error creating storage diff: %w", err)
	}
	return storageDiffID, nil
}

func (repository diffRepository) CreateBackFilledStorageValue(rawDiff types.RawDiff) error {
	_, err := repository.db.Exec(`SELECT * FROM public.create_back_filled_diff($1, $2, $3, $4, $5, $6)`,
		rawDiff.BlockHeight, rawDiff.BlockHash.Bytes(), rawDiff.HashedAddress.Bytes(),
		rawDiff.StorageKey.Bytes(), rawDiff.StorageValue.Bytes(), repository.db.NodeID)
	if err != nil {
		return fmt.Errorf("error creating back filled storage value: %w", err)
	}
	return nil
}

func (repository diffRepository) GetNewDiffs(minID, limit int) ([]types.PersistedDiff, error) {
	var result []types.PersistedDiff
	query := fmt.Sprintf("SELECT id, block_height, block_hash, hashed_address, storage_key, storage_value, eth_node_id FROM public.storage_diff WHERE checked = false and id > %d ORDER BY id ASC LIMIT %d", minID, limit)
	err := repository.db.Select(&result, query)
	if err != nil {
		return nil, fmt.Errorf("error getting unchecked storage diffs with id greater than %d: %w", minID, err)
	}
	return result, nil
}

func (repository diffRepository) MarkChecked(id int64) error {
	_, err := repository.db.Exec(`UPDATE public.storage_diff SET checked = true WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("error marking diff %d checked: %w", id, err)
	}
	return nil
}

func (repository diffRepository) GetFirstDiffIDForBlockHeight(blockHeight int64) (int64, error) {
	var diffID int64
	err := repository.db.Get(&diffID,
		`SELECT id FROM public.storage_diff WHERE block_height >= $1 LIMIT 1`, blockHeight)
	if err != nil {
		return diffID, fmt.Errorf("error getting first diff ID for block height %d: %w", blockHeight, err)
	}
	return diffID, nil
}
