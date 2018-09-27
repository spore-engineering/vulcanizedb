// Copyright 2018 Vulcanize
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package flip_kick_test

import (
	"math/rand"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/vulcanize/vulcanizedb/pkg/core"
	"github.com/vulcanize/vulcanizedb/pkg/fakes"
	"github.com/vulcanize/vulcanizedb/pkg/transformers/flip_kick"
	"github.com/vulcanize/vulcanizedb/pkg/transformers/shared"
	"github.com/vulcanize/vulcanizedb/pkg/transformers/test_data"
	"github.com/vulcanize/vulcanizedb/pkg/transformers/test_data/mocks"
	flip_kick_mocks "github.com/vulcanize/vulcanizedb/pkg/transformers/test_data/mocks/flip_kick"
)

var _ = Describe("FlipKick Transformer", func() {
	var transformer flip_kick.FlipKickTransformer
	var fetcher mocks.MockLogFetcher
	var converter flip_kick_mocks.MockFlipKickConverter
	var repository flip_kick_mocks.MockFlipKickRepository
	var blockNumber int64
	var headerId int64
	var headers []core.Header
	var logs []types.Log

	BeforeEach(func() {
		fetcher = mocks.MockLogFetcher{}
		converter = flip_kick_mocks.MockFlipKickConverter{}
		repository = flip_kick_mocks.MockFlipKickRepository{}
		transformer = flip_kick.FlipKickTransformer{
			Fetcher:    &fetcher,
			Converter:  &converter,
			Repository: &repository,
		}
		transformer.SetConfig(flip_kick.FlipKickConfig)

		blockNumber = rand.Int63()
		headerId = rand.Int63()
		headers = []core.Header{{
			Id:          headerId,
			BlockNumber: blockNumber,
			Hash:        "0x",
			Raw:         nil,
		}}

		repository.SetHeadersToReturn(headers)

		logs = []types.Log{test_data.EthFlipKickLog}
		fetcher.SetFetchedLogs(logs)
	})

	It("fetches logs with the configured contract and topic(s) for each block", func() {
		expectedTopics := [][]common.Hash{{common.HexToHash(shared.FlipKickSignature)}}

		err := transformer.Execute()
		Expect(err).NotTo(HaveOccurred())

		Expect(fetcher.FetchedContractAddresses).To(Equal([][]string{flip_kick.FlipKickConfig.ContractAddresses}))
		Expect(fetcher.FetchedTopics).To(Equal(expectedTopics))
		Expect(fetcher.FetchedBlocks).To(Equal([]int64{blockNumber}))
	})

	It("returns an error if the fetcher fails", func() {
		fetcher.SetFetcherError(fakes.FakeError)

		err := transformer.Execute()
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("error(s) transforming FlipKick event logs"))
	})

	It("marks header checked if no logs returned", func() {
		mockConverter := &flip_kick_mocks.MockFlipKickConverter{}
		mockRepository := &flip_kick_mocks.MockFlipKickRepository{}
		headerID := int64(123)
		mockRepository.SetHeadersToReturn([]core.Header{{Id: headerID}})
		mockFetcher := &mocks.MockLogFetcher{}
		transformer := flip_kick.FlipKickTransformer{
			Converter:  mockConverter,
			Fetcher:    mockFetcher,
			Repository: mockRepository,
		}

		err := transformer.Execute()

		Expect(err).NotTo(HaveOccurred())
		mockRepository.AssertMarkHeaderCheckedCalledWith(headerID)
	})

	It("returns error if marking header checked returns err", func() {
		mockConverter := &flip_kick_mocks.MockFlipKickConverter{}
		mockRepository := &flip_kick_mocks.MockFlipKickRepository{}
		mockRepository.SetHeadersToReturn([]core.Header{{Id: int64(123)}})
		mockRepository.SetMarkHeaderCheckedErr(fakes.FakeError)
		mockFetcher := &mocks.MockLogFetcher{}
		transformer := flip_kick.FlipKickTransformer{
			Converter:  mockConverter,
			Fetcher:    mockFetcher,
			Repository: mockRepository,
		}

		err := transformer.Execute()

		Expect(err).To(HaveOccurred())
		Expect(err).To(MatchError(fakes.FakeError))
	})

	It("converts the logs", func() {
		err := transformer.Execute()
		Expect(err).NotTo(HaveOccurred())

		Expect(converter.ConverterContracts).To(Equal(flip_kick.FlipKickConfig.ContractAddresses))
		Expect(converter.ConverterAbi).To(Equal(flip_kick.FlipKickConfig.ContractAbi))
		Expect(converter.LogsToConvert).To(Equal(logs))
		Expect(converter.EntitiesToConvert).To(Equal([]flip_kick.FlipKickEntity{test_data.FlipKickEntity}))
	})

	It("returns an error if converting the geth log fails", func() {
		converter.SetConverterError(fakes.FakeError)

		err := transformer.Execute()
		Expect(err).To(HaveOccurred())
	})

	It("persists a flip_kick record", func() {
		err := transformer.Execute()
		Expect(err).NotTo(HaveOccurred())

		Expect(repository.HeaderIds).To(Equal([]int64{headerId}))
		Expect(repository.FlipKicksCreated).To(Equal([]flip_kick.FlipKickModel{test_data.FlipKickModel}))
	})

	It("returns an error if persisting a record fails", func() {
		repository.SetCreateRecordError(fakes.FakeError)

		err := transformer.Execute()
		Expect(err).To(HaveOccurred())
	})

	It("returns an error if fetching missing headers fails", func() {
		repository.SetMissingHeadersError(fakes.FakeError)

		err := transformer.Execute()
		Expect(err).To(HaveOccurred())
	})

	It("gets missing headers for blocks between the configured block number range", func() {
		err := transformer.Execute()
		Expect(err).NotTo(HaveOccurred())

		Expect(repository.StartingBlockNumber).To(Equal(flip_kick.FlipKickConfig.StartingBlockNumber))
		Expect(repository.EndingBlockNumber).To(Equal(flip_kick.FlipKickConfig.EndingBlockNumber))
	})
})
