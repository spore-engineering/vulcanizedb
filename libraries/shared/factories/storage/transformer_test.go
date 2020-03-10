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
	"fmt"
	"math/rand"

	"github.com/ethereum/go-ethereum/common"
	"github.com/makerdao/vulcanizedb/libraries/shared/factories/storage"
	"github.com/makerdao/vulcanizedb/libraries/shared/mocks"
	"github.com/makerdao/vulcanizedb/libraries/shared/storage/types"
	"github.com/makerdao/vulcanizedb/pkg/fakes"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Storage transformer", func() {
	var (
		storageKeysLookup *mocks.MockStorageKeysLookup
		repository        *mocks.MockStorageRepository
		t                 storage.Transformer
	)

	BeforeEach(func() {
		storageKeysLookup = &mocks.MockStorageKeysLookup{}
		repository = &mocks.MockStorageRepository{}
		t = storage.Transformer{
			Address:           common.Address{},
			StorageKeysLookup: storageKeysLookup,
			Repository:        repository,
		}
	})

	It("returns the keccaked contract address being watched", func() {
		var (
			MCD_FLIP_ETH_A_address  = "0xd8a04f5412223f513dc55f839574430f5ec15531"
			MCD_FLIP_BAT_A_address  = "0xaa745404d55f88c108a28c86abe7b5a1e7817c07"
			MCD_FLIP_SAI_address  = "0x5432b2f3c0dff95aa191c45e5cbd539e2820ae72"
			MCD_CAT_address  = "0x78f2c2af65126834c51822f56be0d7469d7a523e"
			CDP_MANAGER_address  = "0x5ef30b9986345249bc32d8928b7ee64de9435e39"
			MCD_FLAP_address  = "0xdfe0fb1be2a52cdbf8fb962d5701d7fd0902db9f"
			MCD_FLOP_address  = "0x4d95a049d5b0b7d32058cd3f2163015747522e99"
			MCD_JUG_address  = "0x19c0976f590d67707e62397c87829d896dc0f1f1"
			MCD_POT_address = "0x197e90f9fad81970ba7976f33cbd77088e5d7cf7"
			MCD_SPOT_address  = "0x65c79fcb50ca1594b025960e539ed7a9a6d434a3"
			MCD_VAT_address  = "0x35d1b3f3d7966a1dfe207aa4514c12a259a0492b"
			MCD_VOW_address  = "0xa950524441892a31ebddf91d3ceefa04bf454466"
			OSM_ETH_address  = "0x81FE72B5A8d1A857d176C3E7d5Bd2679A9B85763"
			OSM_BAT_address  = "0xb4eb54af9cc7882df0121d26c5b97e802915abe6"
		)

		contract_addresses := []string{MCD_FLIP_ETH_A_address, MCD_FLIP_BAT_A_address, MCD_FLIP_SAI_address,
			MCD_CAT_address, CDP_MANAGER_address, MCD_FLAP_address, MCD_FLOP_address,
		MCD_JUG_address, MCD_POT_address, MCD_SPOT_address, MCD_VAT_address,
		MCD_VOW_address, OSM_ETH_address, OSM_BAT_address}


		for _, c := range contract_addresses {
			keccakOfAddress := types.HexToKeccak256Hash(c)
			fmt.Println("address: ", c, "keccak", keccakOfAddress.Hex())
		}

		fakeAddress := fakes.FakeAddress
		keccakOfAddress := types.HexToKeccak256Hash(fakeAddress.Hex())
		t.Address = fakeAddress

		Expect(t.KeccakContractAddress()).To(Equal(keccakOfAddress))
	})

	It("returns the contract address being watched", func() {
		fakeAddress := fakes.FakeAddress
		t.Address = fakeAddress

		Expect(t.GetContractAddress()).To(Equal(fakeAddress))
	})

	It("looks up metadata for storage key", func() {
		t.Execute(types.PersistedDiff{})

		Expect(storageKeysLookup.LookupCalled).To(BeTrue())
	})

	It("returns error if lookup fails", func() {
		storageKeysLookup.LookupErr = fakes.FakeError

		err := t.Execute(types.PersistedDiff{})

		Expect(err).To(HaveOccurred())
		Expect(err).To(MatchError(fakes.FakeError))
	})

	It("creates storage row with decoded data", func() {
		fakeMetadata := types.ValueMetadata{Type: types.Address}
		storageKeysLookup.Metadata = fakeMetadata
		rawValue := common.HexToAddress("0x12345")
		fakeHeaderID := rand.Int63()
		fakeBlockNumber := rand.Int()
		fakeBlockHash := fakes.RandomString(64)
		fakeRow := types.PersistedDiff{
			ID:       rand.Int63(),
			HeaderID: fakeHeaderID,
			RawDiff: types.RawDiff{
				HashedAddress: common.Hash{},
				BlockHash:     common.HexToHash(fakeBlockHash),
				BlockHeight:   fakeBlockNumber,
				StorageKey:    common.Hash{},
				StorageValue:  rawValue.Hash(),
			},
		}

		err := t.Execute(fakeRow)

		Expect(err).NotTo(HaveOccurred())
		Expect(repository.PassedHeaderID).To(Equal(fakeHeaderID))
		Expect(repository.PassedDiffID).To(Equal(fakeRow.ID))
		Expect(repository.PassedMetadata).To(Equal(fakeMetadata))
		Expect(repository.PassedValue.(string)).To(Equal(rawValue.Hex()))
	})

	It("returns error if creating row fails", func() {
		rawValue := common.HexToAddress("0x12345")
		fakeMetadata := types.ValueMetadata{Type: types.Address}
		storageKeysLookup.Metadata = fakeMetadata
		repository.CreateErr = fakes.FakeError
		diff := types.PersistedDiff{RawDiff: types.RawDiff{StorageValue: rawValue.Hash()}}

		err := t.Execute(diff)

		Expect(err).To(HaveOccurred())
		Expect(err).To(MatchError(fakes.FakeError))
	})

	Describe("when a storage row contains more than one item packed in storage", func() {
		var (
			rawValue     = common.HexToAddress("000000000000000000000000000000000000000000000002a300000000002a30")
			fakeHeaderID = rand.Int63()
			packedTypes  = make(map[int]types.ValueType)
		)
		packedTypes[0] = types.Uint48
		packedTypes[1] = types.Uint48

		var fakeMetadata = types.ValueMetadata{
			Name:        "",
			Keys:        nil,
			Type:        types.PackedSlot,
			PackedTypes: packedTypes,
		}

		It("passes the decoded data items to the repository", func() {
			storageKeysLookup.Metadata = fakeMetadata
			fakeBlockNumber := rand.Int()
			fakeBlockHash := fakes.RandomString(64)
			fakeRow := types.PersistedDiff{
				ID:       rand.Int63(),
				HeaderID: fakeHeaderID,
				RawDiff: types.RawDiff{
					HashedAddress: common.Hash{},
					BlockHash:     common.HexToHash(fakeBlockHash),
					BlockHeight:   fakeBlockNumber,
					StorageKey:    common.Hash{},
					StorageValue:  rawValue.Hash(),
				},
			}

			err := t.Execute(fakeRow)

			Expect(err).NotTo(HaveOccurred())
			Expect(repository.PassedHeaderID).To(Equal(fakeHeaderID))
			Expect(repository.PassedDiffID).To(Equal(fakeRow.ID))
			Expect(repository.PassedMetadata).To(Equal(fakeMetadata))
			expectedPassedValue := make(map[int]string)
			expectedPassedValue[0] = "10800"
			expectedPassedValue[1] = "172800"
			Expect(repository.PassedValue.(map[int]string)).To(Equal(expectedPassedValue))
		})

		It("returns error if creating a row fails", func() {
			storageKeysLookup.Metadata = fakeMetadata
			repository.CreateErr = fakes.FakeError
			diff := types.PersistedDiff{RawDiff: types.RawDiff{StorageValue: rawValue.Hash()}}

			err := t.Execute(diff)

			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError(fakes.FakeError))
		})
	})
})
