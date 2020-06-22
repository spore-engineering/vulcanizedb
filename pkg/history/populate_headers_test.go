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

package history_test

import (
	"database/sql"
	"math/big"
	"math/rand"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/makerdao/vulcanizedb/pkg/fakes"
	"github.com/makerdao/vulcanizedb/pkg/history"
)

var _ = Describe("Populating headers", func() {
	var (
		blockChain                          *fakes.MockBlockChain
		headerRepository                    *fakes.MockHeaderRepository
		headOfChain, blockBeforeHeadOfChain int64
	)

	BeforeEach(func() {
		blockChain = fakes.NewMockBlockChain()
		headOfChain = rand.Int63()
		blockBeforeHeadOfChain = headOfChain - 1
		blockChain.SetLastBlock(big.NewInt(headOfChain))
		headerRepository = fakes.NewMockHeaderRepository()
	})

	It("returns error if getting last block from chain fails", func() {
		blockChain.SetLastBlockError(fakes.FakeError)

		err := history.PopulateMissingHeaders(blockChain, headerRepository, headOfChain)

		Expect(err).To(HaveOccurred())
		Expect(err).To(MatchError(fakes.FakeError))
	})

	It("returns error if getting missing headers fails", func() {
		headerRepository.MissingBlockNumbersError = fakes.FakeError

		err := history.PopulateMissingHeaders(blockChain, headerRepository, blockBeforeHeadOfChain)

		Expect(err).To(HaveOccurred())
		Expect(err).To(MatchError(fakes.FakeError))
	})

	It("does not error if the db is already synced up to the head of the chain", func() {
		err := history.PopulateMissingHeaders(blockChain, headerRepository, blockBeforeHeadOfChain)

		Expect(err).NotTo(HaveOccurred())
	})

	It("adds missing headers to the db", func() {
		headerRepository.SetMissingBlockNumbers([]int64{headOfChain})

		err := history.PopulateMissingHeaders(blockChain, headerRepository, blockBeforeHeadOfChain)

		Expect(err).NotTo(HaveOccurred())
		headerRepository.AssertCreateOrUpdateHeaderCallCountAndPassedBlockNumbers(1, []int64{headOfChain})
	})

	It("returns error if inserting header fails", func() {
		headerRepository.SetMissingBlockNumbers([]int64{headOfChain})
		headerRepository.SetCreateOrUpdateHeaderReturnErr(fakes.FakeError)

		err := history.PopulateMissingHeaders(blockChain, headerRepository, blockBeforeHeadOfChain)

		Expect(err).To(HaveOccurred())
		Expect(err).To(MatchError(fakes.FakeError))
	})

	It("does not error if scanning header id returns no rows due to duplicate insert", func() {
		headerRepository.SetMissingBlockNumbers([]int64{headOfChain})
		headerRepository.SetCreateOrUpdateHeaderReturnErr(sql.ErrNoRows)

		err := history.PopulateMissingHeaders(blockChain, headerRepository, blockBeforeHeadOfChain)

		Expect(err).NotTo(HaveOccurred())
	})
})
