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

package fakes

type MockCheckedLogsRepository struct {
	AlreadyWatchingLogAddresses []string
	AlreadyWatchingLogError     error
	AlreadyWatchingLogReturn    bool
	AlreadyWatchingLogTopicZero string
	MarkLogWatchedAddresses     []string
	MarkLogWatchedError         error
	MarkLogWatchedTopicZero     string
}

func (repository *MockCheckedLogsRepository) AlreadyWatchingLog(addresses []string, topic0 string) (bool, error) {
	repository.AlreadyWatchingLogAddresses = addresses
	repository.AlreadyWatchingLogTopicZero = topic0
	return repository.AlreadyWatchingLogReturn, repository.AlreadyWatchingLogError
}

func (repository *MockCheckedLogsRepository) MarkLogWatched(addresses []string, topic0 string) error {
	repository.MarkLogWatchedAddresses = addresses
	repository.MarkLogWatchedTopicZero = topic0
	return repository.MarkLogWatchedError
}
