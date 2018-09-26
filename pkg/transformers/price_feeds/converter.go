// Copyright © 2018 Vulcanize
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package price_feeds

import (
	"encoding/json"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
)

type Converter interface {
	ToModels(logs []types.Log, headerID int64) ([]PriceFeedModel, error)
}

type PriceFeedConverter struct{}

func (converter PriceFeedConverter) ToModels(logs []types.Log, headerID int64) (results []PriceFeedModel, err error) {
	for _, log := range logs {
		raw, err := json.Marshal(log)
		if err != nil {
			return nil, err
		}
		model := PriceFeedModel{
			BlockNumber:       log.BlockNumber,
			MedianizerAddress: log.Address.Bytes(),
			UsdValue:          Convert("wad", hexutil.Encode(log.Data), 15),
			TransactionIndex:  log.TxIndex,
			Raw:               raw,
		}
		results = append(results, model)
	}
	return results, err
}