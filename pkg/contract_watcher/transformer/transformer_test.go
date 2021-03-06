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

package transformer_test

import (
	"database/sql"

	"github.com/makerdao/vulcanizedb/pkg/contract_watcher/contract"
	"github.com/makerdao/vulcanizedb/pkg/contract_watcher/helpers/test_helpers/mocks"
	"github.com/makerdao/vulcanizedb/pkg/contract_watcher/parser"
	"github.com/makerdao/vulcanizedb/pkg/contract_watcher/retriever"
	"github.com/makerdao/vulcanizedb/pkg/contract_watcher/transformer"
	"github.com/makerdao/vulcanizedb/pkg/fakes"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Transformer", func() {
	var fakeAddress = "0x1234567890abcdef"
	Describe("Init", func() {
		It("Initializes transformer's contract objects", func() {
			blockRetriever := &fakes.MockBlockRetriever{}
			firstBlock := int64(1)
			blockRetriever.FirstBlock = firstBlock

			parsr := &fakes.MockParser{}
			fakeAbi := "fake_abi"
			parsr.AbiToReturn = fakeAbi

			t := getFakeTransformer(blockRetriever, parsr)

			err := t.Init("")

			Expect(err).ToNot(HaveOccurred())

			c, ok := t.Contracts[fakeAddress]
			Expect(ok).To(Equal(true))

			Expect(c.StartingBlock).To(Equal(firstBlock))
			Expect(c.Abi).To(Equal(fakeAbi))
			Expect(c.Address).To(Equal(fakeAddress))
		})

		It("Fails to initialize if first block cannot be fetched from vDB headers table", func() {
			blockRetriever := &fakes.MockBlockRetriever{}
			blockRetriever.FirstBlockErr = fakes.FakeError
			t := getFakeTransformer(blockRetriever, &fakes.MockParser{})

			err := t.Init("")

			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(fakes.FakeError.Error()))
		})
	})

	Describe("Execute", func() {
		It("Executes contract transformations", func() {
			blockRetriever := &fakes.MockBlockRetriever{}
			firstBlock := int64(1)
			blockRetriever.FirstBlock = firstBlock

			parsr := &fakes.MockParser{}
			fakeAbi := "fake_abi"
			parsr.AbiToReturn = fakeAbi

			t := getFakeTransformer(blockRetriever, parsr)

			err := t.Init("")

			Expect(err).ToNot(HaveOccurred())

			c, ok := t.Contracts[fakeAddress]
			Expect(ok).To(Equal(true))

			Expect(c.StartingBlock).To(Equal(firstBlock))
			Expect(c.Abi).To(Equal(fakeAbi))
			Expect(c.Address).To(Equal(fakeAddress))
		})

		It("uses first block from config if vDB headers table has no rows", func() {
			blockRetriever := &fakes.MockBlockRetriever{}
			blockRetriever.FirstBlockErr = sql.ErrNoRows
			t := getFakeTransformer(blockRetriever, &fakes.MockParser{})

			err := t.Init("")

			Expect(err).ToNot(HaveOccurred())
		})

		It("returns error if fetching first block fails for other reason", func() {
			blockRetriever := &fakes.MockBlockRetriever{}
			blockRetriever.FirstBlockErr = fakes.FakeError
			t := getFakeTransformer(blockRetriever, &fakes.MockParser{})

			err := t.Init("")

			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(fakes.FakeError.Error()))
		})
	})
})

func getFakeTransformer(blockRetriever retriever.BlockRetriever, parsr parser.Parser) transformer.Transformer {
	return transformer.Transformer{
		Parser:           parsr,
		Retriever:        blockRetriever,
		HeaderRepository: &fakes.MockContractWatcherHeaderRepository{},
		Contracts:        map[string]*contract.Contract{},
		Config:           mocks.MockConfig,
	}
}
