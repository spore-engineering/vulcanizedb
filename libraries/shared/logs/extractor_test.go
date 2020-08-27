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

package logs_test

import (
	"math/rand"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/makerdao/vulcanizedb/libraries/shared/constants"
	"github.com/makerdao/vulcanizedb/libraries/shared/factories/event"
	"github.com/makerdao/vulcanizedb/libraries/shared/logs"
	"github.com/makerdao/vulcanizedb/libraries/shared/mocks"
	"github.com/makerdao/vulcanizedb/pkg/core"
	"github.com/makerdao/vulcanizedb/pkg/fakes"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Log extractor", func() {
	var (
		checkedHeadersRepository *fakes.MockCheckedHeadersRepository
		checkedLogsRepository    *fakes.MockCheckedLogsRepository
		extractor                *logs.LogExtractor
		defaultEndingBlockNumber = int64(-1)
	)

	BeforeEach(func() {
		checkedHeadersRepository = &fakes.MockCheckedHeadersRepository{}
		checkedLogsRepository = &fakes.MockCheckedLogsRepository{}
		extractor = &logs.LogExtractor{
			CheckedHeadersRepository: checkedHeadersRepository,
			CheckedLogsRepository:    checkedLogsRepository,
			Fetcher:                  &mocks.MockLogFetcher{},
			LogRepository:            &fakes.MockEventLogRepository{},
			Syncer:                   &fakes.MockTransactionSyncer{},
			RecheckHeaderCap:         constants.RecheckHeaderCap,
		}
	})

	Describe("AddTransformerConfig", func() {
		It("updates extractor's starting block number to earliest available", func() {
			earlierStartingBlockNumber := rand.Int63()
			laterStartingBlockNumber := earlierStartingBlockNumber + 1

			errOne := extractor.AddTransformerConfig(getTransformerConfig(laterStartingBlockNumber, defaultEndingBlockNumber))
			Expect(errOne).NotTo(HaveOccurred())
			errTwo := extractor.AddTransformerConfig(getTransformerConfig(earlierStartingBlockNumber, defaultEndingBlockNumber))
			Expect(errTwo).NotTo(HaveOccurred())

			Expect(*extractor.StartingBlock).To(Equal(earlierStartingBlockNumber))
		})

		It("updates extractor's ending block number to latest available", func() {
			startingBlock := int64(1)
			earlierEndingBlockNumber := rand.Int63()
			laterEndingBlockNumber := earlierEndingBlockNumber + 1

			errOne := extractor.AddTransformerConfig(getTransformerConfig(startingBlock, earlierEndingBlockNumber))
			Expect(errOne).NotTo(HaveOccurred())
			errTwo := extractor.AddTransformerConfig(getTransformerConfig(startingBlock, laterEndingBlockNumber))
			Expect(errTwo).NotTo(HaveOccurred())

			Expect(*extractor.EndingBlock).To(Equal(laterEndingBlockNumber))
		})

		It("treats -1 as the latest ending block number", func() {
			startingBlock := int64(1)
			endingBlockNumber := rand.Int63()
			laterEndingBlockNumber := int64(-1)

			errOne := extractor.AddTransformerConfig(getTransformerConfig(startingBlock, endingBlockNumber))
			Expect(errOne).NotTo(HaveOccurred())
			errTwo := extractor.AddTransformerConfig(getTransformerConfig(startingBlock, laterEndingBlockNumber))
			Expect(errTwo).NotTo(HaveOccurred())
			errThree := extractor.AddTransformerConfig(getTransformerConfig(startingBlock, endingBlockNumber+1))
			Expect(errThree).NotTo(HaveOccurred())

			Expect(*extractor.EndingBlock).To(Equal(laterEndingBlockNumber))
		})

		It("adds transformer's addresses to extractor's watched addresses", func() {
			addresses := []string{"0xA", "0xB"}
			configWithAddresses := event.TransformerConfig{
				ContractAddresses:   addresses,
				StartingBlockNumber: rand.Int63(),
			}

			err := extractor.AddTransformerConfig(configWithAddresses)

			Expect(err).NotTo(HaveOccurred())
			expectedAddresses := event.HexStringsToAddresses(addresses)
			Expect(extractor.Addresses).To(Equal(expectedAddresses))
		})

		It("adds transformer's topic to extractor's watched topics", func() {
			topic := "0x1"
			configWithTopic := event.TransformerConfig{
				ContractAddresses:   []string{fakes.FakeAddress.Hex()},
				Topic:               topic,
				StartingBlockNumber: rand.Int63(),
			}

			err := extractor.AddTransformerConfig(configWithTopic)

			Expect(err).NotTo(HaveOccurred())
			Expect(extractor.Topics).To(Equal([]common.Hash{common.HexToHash(topic)}))
		})

		It("returns error if checking whether log has been checked returns error", func() {
			checkedLogsRepository.AlreadyWatchingLogError = fakes.FakeError

			err := extractor.AddTransformerConfig(getTransformerConfig(rand.Int63(), defaultEndingBlockNumber))

			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError(fakes.FakeError))
		})

		Describe("when log has not previously been checked", func() {
			BeforeEach(func() {
				checkedLogsRepository.AlreadyWatchingLogReturn = false
			})

			It("persists that tranformer's log is watched", func() {
				config := getTransformerConfig(rand.Int63(), defaultEndingBlockNumber)

				err := extractor.AddTransformerConfig(config)

				Expect(err).NotTo(HaveOccurred())
				Expect(checkedLogsRepository.MarkLogWatchedAddresses).To(Equal(config.ContractAddresses))
				Expect(checkedLogsRepository.MarkLogWatchedTopicZero).To(Equal(config.Topic))
			})

			It("returns error if marking logs watched returns error", func() {
				checkedLogsRepository.MarkLogWatchedError = fakes.FakeError

				err := extractor.AddTransformerConfig(getTransformerConfig(rand.Int63(), defaultEndingBlockNumber))

				Expect(err).To(HaveOccurred())
				Expect(err).To(MatchError(fakes.FakeError))
			})
		})
	})

	Describe("ExtractLogs", func() {
		It("returns error if no watched addresses configured", func() {
			err := extractor.ExtractLogs(constants.HeaderUnchecked)

			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError(logs.ErrNoWatchedAddresses))
		})

		Describe("when checking unchecked headers", func() {
			It("gets headers since configured starting block with check_count < 1", func() {
				mockCheckedHeadersRepository := &fakes.MockCheckedHeadersRepository{}
				mockCheckedHeadersRepository.UncheckedHeadersReturnHeaders = []core.Header{{}}
				extractor.CheckedHeadersRepository = mockCheckedHeadersRepository
				startingBlockNumber := rand.Int63()
				addErr := extractor.AddTransformerConfig(getTransformerConfig(startingBlockNumber, defaultEndingBlockNumber))
				Expect(addErr).NotTo(HaveOccurred())

				err := extractor.ExtractLogs(constants.HeaderUnchecked)

				Expect(err).NotTo(HaveOccurred())
				Expect(mockCheckedHeadersRepository.UncheckedHeadersStartingBlockNumber).To(Equal(startingBlockNumber))
				Expect(mockCheckedHeadersRepository.UncheckedHeadersEndingBlockNumber).To(Equal(defaultEndingBlockNumber))
				Expect(mockCheckedHeadersRepository.UncheckedHeadersCheckCount).To(Equal(int64(1)))
			})
		})

		Describe("when rechecking headers", func() {
			It("gets headers since configured starting block with check_count < RecheckHeaderCap", func() {
				mockCheckedHeadersRepository := &fakes.MockCheckedHeadersRepository{}
				mockCheckedHeadersRepository.UncheckedHeadersReturnHeaders = []core.Header{{}}
				extractor.CheckedHeadersRepository = mockCheckedHeadersRepository
				startingBlockNumber := rand.Int63()
				addErr := extractor.AddTransformerConfig(getTransformerConfig(startingBlockNumber, defaultEndingBlockNumber))
				Expect(addErr).NotTo(HaveOccurred())

				err := extractor.ExtractLogs(constants.HeaderRecheck)

				Expect(err).NotTo(HaveOccurred())
				Expect(mockCheckedHeadersRepository.UncheckedHeadersStartingBlockNumber).To(Equal(startingBlockNumber))
				Expect(mockCheckedHeadersRepository.UncheckedHeadersEndingBlockNumber).To(Equal(defaultEndingBlockNumber))
				Expect(mockCheckedHeadersRepository.UncheckedHeadersCheckCount).To(Equal(constants.RecheckHeaderCap))
			})
		})

		It("returns error if getting unchecked headers fails", func() {
			addTransformerConfig(extractor)
			mockCheckedHeadersRepository := &fakes.MockCheckedHeadersRepository{}
			mockCheckedHeadersRepository.UncheckedHeadersReturnError = fakes.FakeError
			extractor.CheckedHeadersRepository = mockCheckedHeadersRepository

			err := extractor.ExtractLogs(constants.HeaderUnchecked)

			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError(fakes.FakeError))
		})

		Describe("when no unchecked headers", func() {
			It("does not fetch logs", func() {
				addTransformerConfig(extractor)
				mockLogFetcher := &mocks.MockLogFetcher{}
				extractor.Fetcher = mockLogFetcher

				_ = extractor.ExtractLogs(constants.HeaderUnchecked)

				Expect(mockLogFetcher.FetchCalled).To(BeFalse())
			})

			It("returns error that no unchecked headers were found", func() {
				addTransformerConfig(extractor)
				mockLogFetcher := &mocks.MockLogFetcher{}
				extractor.Fetcher = mockLogFetcher

				err := extractor.ExtractLogs(constants.HeaderUnchecked)

				Expect(err).To(MatchError(logs.ErrNoUncheckedHeaders))
			})
		})

		Describe("when there are unchecked headers", func() {
			It("fetches logs for unchecked headers", func() {
				addUncheckedHeader(extractor)
				config := event.TransformerConfig{
					ContractAddresses:   []string{fakes.FakeAddress.Hex()},
					Topic:               fakes.FakeHash.Hex(),
					StartingBlockNumber: rand.Int63(),
				}
				addTransformerErr := extractor.AddTransformerConfig(config)
				Expect(addTransformerErr).NotTo(HaveOccurred())
				mockLogFetcher := &mocks.MockLogFetcher{}
				extractor.Fetcher = mockLogFetcher

				err := extractor.ExtractLogs(constants.HeaderUnchecked)

				Expect(err).NotTo(HaveOccurred())
				Expect(mockLogFetcher.FetchCalled).To(BeTrue())
				expectedTopics := []common.Hash{common.HexToHash(config.Topic)}
				Expect(mockLogFetcher.Topics).To(Equal(expectedTopics))
				expectedAddresses := event.HexStringsToAddresses(config.ContractAddresses)
				Expect(mockLogFetcher.ContractAddresses).To(Equal(expectedAddresses))
			})

			It("returns error if fetching logs fails", func() {
				addUncheckedHeader(extractor)
				addTransformerConfig(extractor)
				mockLogFetcher := &mocks.MockLogFetcher{}
				mockLogFetcher.ReturnError = fakes.FakeError
				extractor.Fetcher = mockLogFetcher

				err := extractor.ExtractLogs(constants.HeaderUnchecked)

				Expect(err).To(HaveOccurred())
				Expect(err).To(MatchError(fakes.FakeError))
			})

			Describe("when no fetched logs", func() {
				It("does not sync transactions", func() {
					addUncheckedHeader(extractor)
					addTransformerConfig(extractor)
					mockTransactionSyncer := &fakes.MockTransactionSyncer{}
					extractor.Syncer = mockTransactionSyncer

					err := extractor.ExtractLogs(constants.HeaderUnchecked)

					Expect(err).NotTo(HaveOccurred())
					Expect(mockTransactionSyncer.SyncTransactionsCalled).To(BeFalse())
				})
			})

			Describe("when there are fetched logs", func() {
				It("syncs transactions", func() {
					addUncheckedHeader(extractor)
					addFetchedLog(extractor)
					addTransformerConfig(extractor)
					mockTransactionSyncer := &fakes.MockTransactionSyncer{}
					extractor.Syncer = mockTransactionSyncer

					err := extractor.ExtractLogs(constants.HeaderUnchecked)

					Expect(err).NotTo(HaveOccurred())
					Expect(mockTransactionSyncer.SyncTransactionsCalled).To(BeTrue())
				})

				It("returns error if syncing transactions fails", func() {
					addUncheckedHeader(extractor)
					addFetchedLog(extractor)
					addTransformerConfig(extractor)
					mockTransactionSyncer := &fakes.MockTransactionSyncer{}
					mockTransactionSyncer.SyncTransactionsError = fakes.FakeError
					extractor.Syncer = mockTransactionSyncer

					err := extractor.ExtractLogs(constants.HeaderUnchecked)

					Expect(err).To(HaveOccurred())
					Expect(err).To(MatchError(fakes.FakeError))
				})

				It("persists fetched logs", func() {
					addUncheckedHeader(extractor)
					addTransformerConfig(extractor)
					fakeLogs := []types.Log{{
						Address: common.HexToAddress("0xA"),
						Topics:  []common.Hash{common.HexToHash("0xA")},
						Data:    []byte{},
						Index:   0,
					}}
					mockLogFetcher := &mocks.MockLogFetcher{ReturnLogs: fakeLogs}
					extractor.Fetcher = mockLogFetcher
					mockLogRepository := &fakes.MockEventLogRepository{}
					extractor.LogRepository = mockLogRepository

					err := extractor.ExtractLogs(constants.HeaderUnchecked)

					Expect(err).NotTo(HaveOccurred())
					Expect(mockLogRepository.PassedLogs).To(Equal(fakeLogs))
				})

				It("returns error if persisting logs fails", func() {
					addUncheckedHeader(extractor)
					addFetchedLog(extractor)
					addTransformerConfig(extractor)
					mockLogRepository := &fakes.MockEventLogRepository{}
					mockLogRepository.CreateError = fakes.FakeError
					extractor.LogRepository = mockLogRepository

					err := extractor.ExtractLogs(constants.HeaderUnchecked)

					Expect(err).To(HaveOccurred())
					Expect(err).To(MatchError(fakes.FakeError))
				})
			})

			It("marks header checked", func() {
				addFetchedLog(extractor)
				addTransformerConfig(extractor)
				mockCheckedHeadersRepository := &fakes.MockCheckedHeadersRepository{}
				headerID := rand.Int63()
				mockCheckedHeadersRepository.UncheckedHeadersReturnHeaders = []core.Header{{Id: headerID}}
				extractor.CheckedHeadersRepository = mockCheckedHeadersRepository

				err := extractor.ExtractLogs(constants.HeaderUnchecked)

				Expect(err).NotTo(HaveOccurred())
				Expect(mockCheckedHeadersRepository.MarkHeaderCheckedHeaderID).To(Equal(headerID))
			})

			It("returns error if marking header checked fails", func() {
				addFetchedLog(extractor)
				addTransformerConfig(extractor)
				mockCheckedHeadersRepository := &fakes.MockCheckedHeadersRepository{}
				mockCheckedHeadersRepository.UncheckedHeadersReturnHeaders = []core.Header{{Id: rand.Int63()}}
				mockCheckedHeadersRepository.MarkHeaderCheckedReturnError = fakes.FakeError
				extractor.CheckedHeadersRepository = mockCheckedHeadersRepository

				err := extractor.ExtractLogs(constants.HeaderUnchecked)

				Expect(err).To(HaveOccurred())
				Expect(err).To(MatchError(fakes.FakeError))
			})

			It("returns nil for error if everything succeeds", func() {
				addUncheckedHeader(extractor)
				addTransformerConfig(extractor)

				err := extractor.ExtractLogs(constants.HeaderUnchecked)

				Expect(err).NotTo(HaveOccurred())
			})
		})
	})

	Describe("BackFillLogs", func() {
		It("returns error if no watched addresses configured", func() {
			err := extractor.BackFillLogs(0)

			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError(logs.ErrNoWatchedAddresses))
		})

		It("gets headers from transformer's starting block through passed ending block", func() {
			startingBlock := addTransformerConfig(extractor)
			mockHeaderRepository := &fakes.MockHeaderRepository{}
			extractor.HeaderRepository = mockHeaderRepository
			endingBlock := startingBlock + 1

			_ = extractor.BackFillLogs(endingBlock)

			Expect(mockHeaderRepository.GetHeadersInRangeStartingBlocks).To(ContainElement(startingBlock))
			Expect(mockHeaderRepository.GetHeadersInRangeEndingBlocks).To(ContainElement(endingBlock))
		})

		It("chunks headers if range greater than interval defined by HeaderChunkSize", func() {
			startingBlock := addTransformerConfig(extractor)
			mockHeaderRepository := &fakes.MockHeaderRepository{}
			extractor.HeaderRepository = mockHeaderRepository
			endingBlock := startingBlock + logs.HeaderChunkSize*2

			err := extractor.BackFillLogs(endingBlock)

			Expect(err).NotTo(HaveOccurred())
			Expect(mockHeaderRepository.GetHeadersInRangeStartingBlocks).To(ConsistOf([]int64{
				startingBlock,
				startingBlock + logs.HeaderChunkSize,
			}))
			Expect(mockHeaderRepository.GetHeadersInRangeEndingBlocks).To(ConsistOf([]int64{
				startingBlock + logs.HeaderChunkSize - 1,
				startingBlock + logs.HeaderChunkSize*2 - 1,
			}))
		})

		It("returns error if getting headers in range returns error", func() {
			mockHeaderRepository := &fakes.MockHeaderRepository{}
			mockHeaderRepository.GetHeadersInRangeError = fakes.FakeError
			extractor.HeaderRepository = mockHeaderRepository
			startingBlock := addTransformerConfig(extractor)

			err := extractor.BackFillLogs(startingBlock + 1)

			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError(fakes.FakeError))
		})

		It("does nothing if no headers found", func() {
			mockHeaderRepository := &fakes.MockHeaderRepository{}
			extractor.HeaderRepository = mockHeaderRepository
			startingBlock := addTransformerConfig(extractor)
			mockLogFetcher := &mocks.MockLogFetcher{}
			extractor.Fetcher = mockLogFetcher

			_ = extractor.BackFillLogs(startingBlock + 1)

			Expect(mockLogFetcher.FetchCalled).To(BeFalse())
		})

		It("fetches logs for headers in range", func() {
			addHeaderInRange(extractor)
			config := event.TransformerConfig{
				ContractAddresses:   []string{fakes.FakeAddress.Hex()},
				Topic:               fakes.FakeHash.Hex(),
				StartingBlockNumber: rand.Int63(),
			}
			addTransformerErr := extractor.AddTransformerConfig(config)
			Expect(addTransformerErr).NotTo(HaveOccurred())
			mockLogFetcher := &mocks.MockLogFetcher{}
			extractor.Fetcher = mockLogFetcher

			err := extractor.BackFillLogs(config.StartingBlockNumber + 1)

			Expect(err).NotTo(HaveOccurred())
			Expect(mockLogFetcher.FetchCalled).To(BeTrue())
			expectedTopics := []common.Hash{common.HexToHash(config.Topic)}
			Expect(mockLogFetcher.Topics).To(Equal(expectedTopics))
			expectedAddresses := event.HexStringsToAddresses(config.ContractAddresses)
			Expect(mockLogFetcher.ContractAddresses).To(Equal(expectedAddresses))
		})

		It("returns error if fetching logs fails", func() {
			addHeaderInRange(extractor)
			startingBlock := addTransformerConfig(extractor)
			mockLogFetcher := &mocks.MockLogFetcher{}
			mockLogFetcher.ReturnError = fakes.FakeError
			extractor.Fetcher = mockLogFetcher

			err := extractor.BackFillLogs(startingBlock + 1)

			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError(fakes.FakeError))
		})

		It("does not sync transactions when no logs", func() {
			addHeaderInRange(extractor)
			startingBlock := addTransformerConfig(extractor)
			mockTransactionSyncer := &fakes.MockTransactionSyncer{}
			extractor.Syncer = mockTransactionSyncer

			err := extractor.BackFillLogs(startingBlock + 1)

			Expect(err).NotTo(HaveOccurred())
			Expect(mockTransactionSyncer.SyncTransactionsCalled).To(BeFalse())
		})

		Describe("when there are fetched logs", func() {
			It("syncs transactions", func() {
				addHeaderInRange(extractor)
				addFetchedLog(extractor)
				startingBlock := addTransformerConfig(extractor)
				mockTransactionSyncer := &fakes.MockTransactionSyncer{}
				extractor.Syncer = mockTransactionSyncer

				err := extractor.BackFillLogs(startingBlock + 1)

				Expect(err).NotTo(HaveOccurred())
				Expect(mockTransactionSyncer.SyncTransactionsCalled).To(BeTrue())
			})

			It("returns error if syncing transactions fails", func() {
				addHeaderInRange(extractor)
				addFetchedLog(extractor)
				startingBlock := addTransformerConfig(extractor)
				mockTransactionSyncer := &fakes.MockTransactionSyncer{}
				mockTransactionSyncer.SyncTransactionsError = fakes.FakeError
				extractor.Syncer = mockTransactionSyncer

				err := extractor.BackFillLogs(startingBlock + 1)

				Expect(err).To(HaveOccurred())
				Expect(err).To(MatchError(fakes.FakeError))
			})

			It("persists fetched logs", func() {
				addHeaderInRange(extractor)
				startingBlock := addTransformerConfig(extractor)
				fakeLogs := []types.Log{{
					Address: common.HexToAddress("0xA"),
					Topics:  []common.Hash{common.HexToHash("0xA")},
					Data:    []byte{},
					Index:   0,
				}}
				mockLogFetcher := &mocks.MockLogFetcher{ReturnLogs: fakeLogs}
				extractor.Fetcher = mockLogFetcher
				mockLogRepository := &fakes.MockEventLogRepository{}
				extractor.LogRepository = mockLogRepository

				err := extractor.BackFillLogs(startingBlock + 1)

				Expect(err).NotTo(HaveOccurred())
				Expect(mockLogRepository.PassedLogs).To(Equal(fakeLogs))
			})

			It("returns error if persisting logs fails", func() {
				addHeaderInRange(extractor)
				addFetchedLog(extractor)
				startingBlock := addTransformerConfig(extractor)
				mockLogRepository := &fakes.MockEventLogRepository{}
				mockLogRepository.CreateError = fakes.FakeError
				extractor.LogRepository = mockLogRepository

				err := extractor.BackFillLogs(startingBlock + 1)

				Expect(err).To(HaveOccurred())
				Expect(err).To(MatchError(fakes.FakeError))
			})
		})
	})

	Describe("ChunkRanges", func() {
		It("returns error if upper bound <= lower bound", func() {
			_, err := logs.ChunkRanges(10, 10, 1)

			Expect(err).To(HaveOccurred())
		})

		It("chunks items in range by interval", func() {
			res, err := logs.ChunkRanges(1, 20, 10)

			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(ConsistOf([]map[logs.BlockIdentifier]int64{
				{logs.StartInterval: 1, logs.EndInterval: 10},
				{logs.StartInterval: 11, logs.EndInterval: 20},
			}))
		})

		It("truncates final chunk to end at upper bound", func() {
			res, err := logs.ChunkRanges(1, 25, 10)

			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(ConsistOf([]map[logs.BlockIdentifier]int64{
				{logs.StartInterval: 1, logs.EndInterval: 10},
				{logs.StartInterval: 11, logs.EndInterval: 20},
				{logs.StartInterval: 21, logs.EndInterval: 25},
			}))
		})
	})
})

func addTransformerConfig(extractor *logs.LogExtractor) int64 {
	fakeConfig := event.TransformerConfig{
		ContractAddresses:   []string{fakes.FakeAddress.Hex()},
		Topic:               fakes.FakeHash.Hex(),
		StartingBlockNumber: rand.Int63(),
	}
	addErr := extractor.AddTransformerConfig(fakeConfig)
	Expect(addErr).NotTo(HaveOccurred())
	return fakeConfig.StartingBlockNumber
}

func addUncheckedHeader(extractor *logs.LogExtractor) {
	mockCheckedHeadersRepository := &fakes.MockCheckedHeadersRepository{}
	mockCheckedHeadersRepository.UncheckedHeadersReturnHeaders = []core.Header{{}}
	extractor.CheckedHeadersRepository = mockCheckedHeadersRepository
}

func addHeaderInRange(extractor *logs.LogExtractor) {
	mockHeadersRepository := &fakes.MockHeaderRepository{}
	mockHeadersRepository.AllHeaders = []core.Header{{}}
	extractor.HeaderRepository = mockHeadersRepository
}

func addFetchedLog(extractor *logs.LogExtractor) {
	mockLogFetcher := &mocks.MockLogFetcher{}
	mockLogFetcher.ReturnLogs = []types.Log{{}}
	extractor.Fetcher = mockLogFetcher
}

func getTransformerConfig(startingBlockNumber, endingBlockNumber int64) event.TransformerConfig {
	return event.TransformerConfig{
		ContractAddresses:   []string{fakes.FakeAddress.Hex()},
		Topic:               fakes.FakeHash.Hex(),
		StartingBlockNumber: startingBlockNumber,
		EndingBlockNumber:   endingBlockNumber,
	}
}
