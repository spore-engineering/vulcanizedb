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

package storage_test

import (
	"database/sql"
	"math/rand"

	"github.com/ethereum/go-ethereum/common"
	"github.com/makerdao/vulcanizedb/libraries/shared/storage"
	"github.com/makerdao/vulcanizedb/libraries/shared/storage/types"
	"github.com/makerdao/vulcanizedb/libraries/shared/test_data"
	"github.com/makerdao/vulcanizedb/test_config"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Storage diffs repository", func() {
	var (
		db              = test_config.NewTestDB(test_config.NewTestNode())
		repo            storage.DiffRepository
		fakeStorageDiff types.RawDiff
	)

	BeforeEach(func() {
		test_config.CleanTestDB(db)
		repo = storage.NewDiffRepository(db)
		fakeStorageDiff = types.RawDiff{
			HashedAddress: test_data.FakeHash(),
			BlockHash:     test_data.FakeHash(),
			BlockHeight:   rand.Int(),
			StorageKey:    test_data.FakeHash(),
			StorageValue:  test_data.FakeHash(),
		}
	})

	type dbStorageDiff struct {
		Created string
		Updated string
	}

	Describe("CreateStorageDiff", func() {
		It("adds a storage diff to the db, returning id", func() {
			id, createErr := repo.CreateStorageDiff(fakeStorageDiff)

			Expect(createErr).NotTo(HaveOccurred())
			Expect(id).NotTo(BeZero())
			var persisted types.PersistedDiff
			getErr := db.Get(&persisted, `SELECT id, hashed_address, block_hash, block_height, storage_key, storage_value FROM public.storage_diff`)
			Expect(getErr).NotTo(HaveOccurred())
			Expect(persisted.ID).To(Equal(id))
			Expect(persisted.HashedAddress).To(Equal(fakeStorageDiff.HashedAddress))
			Expect(persisted.BlockHash).To(Equal(fakeStorageDiff.BlockHash))
			Expect(persisted.BlockHeight).To(Equal(fakeStorageDiff.BlockHeight))
			Expect(persisted.StorageKey).To(Equal(fakeStorageDiff.StorageKey))
			Expect(persisted.StorageValue).To(Equal(fakeStorageDiff.StorageValue))
		})

		It("does not duplicate storage diffs", func() {
			_, createErr := repo.CreateStorageDiff(fakeStorageDiff)
			Expect(createErr).NotTo(HaveOccurred())

			_, createTwoErr := repo.CreateStorageDiff(fakeStorageDiff)
			Expect(createTwoErr).To(HaveOccurred())
			Expect(createTwoErr).To(MatchError(sql.ErrNoRows))

			var count int
			getErr := db.Get(&count, `SELECT count(*) FROM public.storage_diff`)
			Expect(getErr).NotTo(HaveOccurred())
			Expect(count).To(Equal(1))
		})

		It("indicates when a record was created or updated", func() {
			id, createErr := repo.CreateStorageDiff(fakeStorageDiff)
			Expect(createErr).NotTo(HaveOccurred())

			var storageDiffUpdatedRes dbStorageDiff
			initialStorageErr := db.Get(&storageDiffUpdatedRes, `SELECT created, updated FROM public.storage_diff`)
			Expect(initialStorageErr).NotTo(HaveOccurred())
			Expect(storageDiffUpdatedRes.Created).To(Equal(storageDiffUpdatedRes.Updated))

			_, updateErr := db.Exec(`UPDATE public.storage_diff SET block_hash = '{"new_block_hash"}' where id = $1`, id)
			Expect(updateErr).NotTo(HaveOccurred())
			updatedDiffErr := db.Get(&storageDiffUpdatedRes, `SELECT created, updated FROM public.storage_diff`)
			Expect(updatedDiffErr).NotTo(HaveOccurred())
			Expect(storageDiffUpdatedRes.Created).NotTo(Equal(storageDiffUpdatedRes.Updated))
		})
	})

	Describe("CreateBackFilledStorageValue", func() {
		It("creates a storage diff", func() {
			createErr := repo.CreateBackFilledStorageValue(fakeStorageDiff)

			Expect(createErr).NotTo(HaveOccurred())
			var persisted types.PersistedDiff
			getErr := db.Get(&persisted, `SELECT hashed_address, block_hash, block_height, storage_key, storage_value FROM public.storage_diff`)
			Expect(getErr).NotTo(HaveOccurred())
			Expect(persisted.HashedAddress).To(Equal(fakeStorageDiff.HashedAddress))
			Expect(persisted.BlockHash).To(Equal(fakeStorageDiff.BlockHash))
			Expect(persisted.BlockHeight).To(Equal(fakeStorageDiff.BlockHeight))
			Expect(persisted.StorageKey).To(Equal(fakeStorageDiff.StorageKey))
			Expect(persisted.StorageValue).To(Equal(fakeStorageDiff.StorageValue))
		})

		It("marks diff as back-filled", func() {
			createErr := repo.CreateBackFilledStorageValue(fakeStorageDiff)

			Expect(createErr).NotTo(HaveOccurred())
			var fromBackfill bool
			checkedErr := db.Get(&fromBackfill, `SELECT from_backfill FROM public.storage_diff`)
			Expect(checkedErr).NotTo(HaveOccurred())
			Expect(fromBackfill).To(BeTrue())
		})

		It("does not duplicate storage values in the same block", func() {
			_, createErr := repo.CreateStorageDiff(fakeStorageDiff)
			Expect(createErr).NotTo(HaveOccurred())

			createTwoErr := repo.CreateBackFilledStorageValue(fakeStorageDiff)
			Expect(createTwoErr).NotTo(HaveOccurred())

			var count int
			getErr := db.Get(&count, `SELECT count(*) FROM public.storage_diff`)
			Expect(getErr).NotTo(HaveOccurred())
			Expect(count).To(Equal(1))
		})

		It("does not duplicate storage values across subsequent blocks", func() {
			_, createErr := repo.CreateStorageDiff(fakeStorageDiff)
			Expect(createErr).NotTo(HaveOccurred())

			duplicateDiff := fakeStorageDiff
			duplicateDiff.BlockHeight = fakeStorageDiff.BlockHeight + 1
			createTwoErr := repo.CreateBackFilledStorageValue(duplicateDiff)
			Expect(createTwoErr).NotTo(HaveOccurred())

			var count int
			getErr := db.Get(&count, `SELECT count(*) FROM public.storage_diff`)
			Expect(getErr).NotTo(HaveOccurred())
			Expect(count).To(Equal(1))
		})

		It("does duplicate storage value if same value only exists at a later block", func() {
			_, createErr := repo.CreateStorageDiff(fakeStorageDiff)
			Expect(createErr).NotTo(HaveOccurred())

			duplicateDiff := fakeStorageDiff
			duplicateDiff.BlockHeight = fakeStorageDiff.BlockHeight - 1
			createTwoErr := repo.CreateBackFilledStorageValue(duplicateDiff)
			Expect(createTwoErr).NotTo(HaveOccurred())

			var count int
			getErr := db.Get(&count, `SELECT count(*) FROM public.storage_diff`)
			Expect(getErr).NotTo(HaveOccurred())
			Expect(count).To(Equal(2))
		})

		It("inserts zero-valued storage if there's a previous diff", func() {
			_, createOneErr := repo.CreateStorageDiff(fakeStorageDiff)
			Expect(createOneErr).NotTo(HaveOccurred())
			emptyStorageValue := fakeStorageDiff
			emptyStorageValue.StorageValue = common.HexToHash("0x0")

			createTwoErr := repo.CreateBackFilledStorageValue(emptyStorageValue)

			Expect(createTwoErr).NotTo(HaveOccurred())
			var count int
			getErr := db.Get(&count, `SELECT count(*) FROM public.storage_diff`)
			Expect(getErr).NotTo(HaveOccurred())
			Expect(count).To(Equal(2))
		})

		It("does not insert zero-valued storage if there's no previous diff", func() {
			emptyStorageValue := fakeStorageDiff
			emptyStorageValue.StorageValue = common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000000")

			createErr := repo.CreateBackFilledStorageValue(emptyStorageValue)

			Expect(createErr).NotTo(HaveOccurred())
			var count int
			getErr := db.Get(&count, `SELECT count(*) FROM public.storage_diff`)
			Expect(getErr).NotTo(HaveOccurred())
			Expect(count).To(BeZero())
		})

		It("does not insert zero-valued storage derived from bytes if there's no previous diff", func() {
			emptyStorageValue := fakeStorageDiff
			emptyStorageValue.StorageValue = common.BytesToHash([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})

			createErr := repo.CreateBackFilledStorageValue(emptyStorageValue)

			Expect(createErr).NotTo(HaveOccurred())
			var count int
			getErr := db.Get(&count, `SELECT count(*) FROM public.storage_diff`)
			Expect(getErr).NotTo(HaveOccurred())

		})
	})

	Describe("GetNewDiffs", func() {
		It("sends diffs that are not marked as checked", func() {
			fakeRawDiff := types.RawDiff{
				HashedAddress: test_data.FakeHash(),
				BlockHash:     test_data.FakeHash(),
				BlockHeight:   rand.Int(),
				StorageKey:    test_data.FakeHash(),
				StorageValue:  test_data.FakeHash(),
			}
			fakePersistedDiff := types.PersistedDiff{
				RawDiff:   fakeRawDiff,
				ID:        rand.Int63(),
				EthNodeID: db.NodeID,
			}
			_, insertErr := db.Exec(`INSERT INTO public.storage_diff (id, block_height, block_hash,
				hashed_address, storage_key, storage_value, eth_node_id) VALUES ($1, $2, $3, $4, $5, $6, $7)`, fakePersistedDiff.ID,
				fakeRawDiff.BlockHeight, fakeRawDiff.BlockHash.Bytes(), fakeRawDiff.HashedAddress.Bytes(),
				fakeRawDiff.StorageKey.Bytes(), fakeRawDiff.StorageValue.Bytes(), fakePersistedDiff.EthNodeID)
			Expect(insertErr).NotTo(HaveOccurred())

			diffs, err := repo.GetNewDiffs(0, 1)

			Expect(err).NotTo(HaveOccurred())
			Expect(diffs).To(ConsistOf(fakePersistedDiff))
		})

		It("does not send diff that's marked as checked", func() {
			fakeRawDiff := types.RawDiff{
				HashedAddress: test_data.FakeHash(),
				BlockHash:     test_data.FakeHash(),
				BlockHeight:   rand.Int(),
				StorageKey:    test_data.FakeHash(),
				StorageValue:  test_data.FakeHash(),
			}
			fakePersistedDiff := types.PersistedDiff{
				RawDiff:   fakeRawDiff,
				ID:        rand.Int63(),
				Checked:   true,
				EthNodeID: db.NodeID,
			}
			_, insertErr := db.Exec(`INSERT INTO public.storage_diff (id, block_height, block_hash,
				hashed_address, storage_key, storage_value, checked, eth_node_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
				fakePersistedDiff.ID, fakeRawDiff.BlockHeight, fakeRawDiff.BlockHash.Bytes(),
				fakeRawDiff.HashedAddress.Bytes(), fakeRawDiff.StorageKey.Bytes(), fakeRawDiff.StorageValue.Bytes(),
				fakePersistedDiff.Checked, fakePersistedDiff.EthNodeID)
			Expect(insertErr).NotTo(HaveOccurred())

			diffs, err := repo.GetNewDiffs(0, 1)

			Expect(err).NotTo(HaveOccurred())
			Expect(diffs).To(BeEmpty())
		})

		It("enables seeking diffs with greater ID", func() {
			blockZero := rand.Int()
			nodeID := db.NodeID
			for i := 0; i < 2; i++ {
				fakeRawDiff := types.RawDiff{
					HashedAddress: test_data.FakeHash(),
					BlockHash:     test_data.FakeHash(),
					BlockHeight:   blockZero + i,
					StorageKey:    test_data.FakeHash(),
					StorageValue:  test_data.FakeHash(),
				}
				_, insertErr := db.Exec(`INSERT INTO public.storage_diff (block_height, block_hash,
				hashed_address, storage_key, storage_value, eth_node_id) VALUES ($1, $2, $3, $4, $5, $6)`, fakeRawDiff.BlockHeight,
					fakeRawDiff.BlockHash.Bytes(), fakeRawDiff.HashedAddress.Bytes(), fakeRawDiff.StorageKey.Bytes(),
					fakeRawDiff.StorageValue.Bytes(), nodeID)
				Expect(insertErr).NotTo(HaveOccurred())
			}

			minID := 0
			limit := 1
			diffsOne, errOne := repo.GetNewDiffs(minID, limit)
			Expect(errOne).NotTo(HaveOccurred())
			Expect(len(diffsOne)).To(Equal(1))
			nextID := int(diffsOne[0].ID)
			diffsTwo, errTwo := repo.GetNewDiffs(nextID, limit)
			Expect(errTwo).NotTo(HaveOccurred())
			Expect(len(diffsTwo)).To(Equal(1))
			Expect(int(diffsTwo[0].ID) > nextID).To(BeTrue())
		})
	})

	Describe("MarkChecked", func() {
		It("marks a diff as checked", func() {
			fakeRawDiff := types.RawDiff{
				HashedAddress: test_data.FakeHash(),
				BlockHash:     test_data.FakeHash(),
				BlockHeight:   rand.Int(),
				StorageKey:    test_data.FakeHash(),
				StorageValue:  test_data.FakeHash(),
			}
			fakePersistedDiff := types.PersistedDiff{
				RawDiff:   fakeRawDiff,
				ID:        rand.Int63(),
				Checked:   false,
				EthNodeID: db.NodeID,
			}
			_, insertErr := db.Exec(`INSERT INTO public.storage_diff (id, block_height, block_hash,
				hashed_address, storage_key, storage_value, checked, eth_node_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
				fakePersistedDiff.ID, fakeRawDiff.BlockHeight, fakeRawDiff.BlockHash.Bytes(),
				fakeRawDiff.HashedAddress.Bytes(), fakeRawDiff.StorageKey.Bytes(), fakeRawDiff.StorageValue.Bytes(),
				fakePersistedDiff.Checked, fakePersistedDiff.EthNodeID)
			Expect(insertErr).NotTo(HaveOccurred())

			err := repo.MarkChecked(fakePersistedDiff.ID)

			Expect(err).NotTo(HaveOccurred())
			var checked bool
			checkedErr := db.Get(&checked, `SELECT checked FROM public.storage_diff WHERE id = $1`, fakePersistedDiff.ID)
			Expect(checkedErr).NotTo(HaveOccurred())
			Expect(checked).To(BeTrue())
		})
	})

	Describe("GetFirstDiffIDForBlockHeight", func() {
		It("sends first diff for a given block height", func() {
			blockHeight := fakeStorageDiff.BlockHeight
			fakeStorageDiff2 := types.RawDiff{
				HashedAddress: test_data.FakeHash(),
				BlockHash:     test_data.FakeHash(),
				BlockHeight:   blockHeight,
				StorageKey:    test_data.FakeHash(),
				StorageValue:  test_data.FakeHash(),
			}

			id1, create1Err := repo.CreateStorageDiff(fakeStorageDiff)
			Expect(create1Err).NotTo(HaveOccurred())
			_, create2Err := repo.CreateStorageDiff(fakeStorageDiff2)
			Expect(create2Err).NotTo(HaveOccurred())

			diffID, diffErr := repo.GetFirstDiffIDForBlockHeight(int64(blockHeight))
			Expect(diffErr).NotTo(HaveOccurred())
			Expect(diffID).To(Equal(id1))
		})

		It("sends a diff for the next block height if one doesn't exist for the block passed in", func() {
			blockHeight := fakeStorageDiff.BlockHeight
			fakeStorageDiff2 := types.RawDiff{
				HashedAddress: test_data.FakeHash(),
				BlockHash:     test_data.FakeHash(),
				BlockHeight:   blockHeight,
				StorageKey:    test_data.FakeHash(),
				StorageValue:  test_data.FakeHash(),
			}

			id1, create1Err := repo.CreateStorageDiff(fakeStorageDiff)
			Expect(create1Err).NotTo(HaveOccurred())
			_, create2Err := repo.CreateStorageDiff(fakeStorageDiff2)
			Expect(create2Err).NotTo(HaveOccurred())

			blockBeforeDiffBlockHeight := int64(blockHeight - 1)
			diffID, diffErr := repo.GetFirstDiffIDForBlockHeight(blockBeforeDiffBlockHeight)
			Expect(diffErr).NotTo(HaveOccurred())
			Expect(diffID).To(Equal(id1))
		})

		It("won't fail if all of the diffs within the id range are already checked", func() {
			fakeRawDiff := types.RawDiff{
				HashedAddress: test_data.FakeHash(),
				BlockHash:     test_data.FakeHash(),
				BlockHeight:   rand.Int(),
				StorageKey:    test_data.FakeHash(),
				StorageValue:  test_data.FakeHash(),
			}
			fakePersistedDiff := types.PersistedDiff{
				RawDiff:   fakeRawDiff,
				ID:        rand.Int63(),
				Checked:   true,
				EthNodeID: db.NodeID,
			}
			_, insertErr := db.Exec(`INSERT INTO public.storage_diff (id, block_height, block_hash,
				hashed_address, storage_key, storage_value, checked, eth_node_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
				fakePersistedDiff.ID, fakeRawDiff.BlockHeight, fakeRawDiff.BlockHash.Bytes(),
				fakeRawDiff.HashedAddress.Bytes(), fakeRawDiff.StorageKey.Bytes(), fakeRawDiff.StorageValue.Bytes(),
				fakePersistedDiff.Checked, fakePersistedDiff.EthNodeID)
			Expect(insertErr).NotTo(HaveOccurred())

			var insertedDiffID int64
			getInsertedDiffIDErr := db.Get(&insertedDiffID, `SELECT id FROM storage_diff LIMIT 1`)
			Expect(getInsertedDiffIDErr).NotTo(HaveOccurred())

			blockBeforeDiffBlockHeight := int64(fakeRawDiff.BlockHeight - 1)
			diffID, diffErr := repo.GetFirstDiffIDForBlockHeight(blockBeforeDiffBlockHeight)
			Expect(diffErr).NotTo(HaveOccurred())
			Expect(diffID).To(Equal(insertedDiffID))
		})

		It("returns an error if getting the diff fails", func() {
			_, diffErr := repo.GetFirstDiffIDForBlockHeight(0)
			Expect(diffErr).To(HaveOccurred())
			Expect(diffErr).To(MatchError(sql.ErrNoRows))
		})
	})
})
