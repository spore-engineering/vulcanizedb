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

package test_data

import (
	"encoding/json"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/vulcanize/vulcanizedb/pkg/transformers/deal"
	"github.com/vulcanize/vulcanizedb/pkg/transformers/shared"
)

var DealLogNote = types.Log{
	Address: common.HexToAddress(shared.FlipperContractAddress),
	Topics: []common.Hash{
		common.HexToHash("0xc959c42b00000000000000000000000000000000000000000000000000000000"),
		common.HexToHash("0x00000000000000000000000064d922894153be9eef7b7218dc565d1d0ce2a092"),
		common.HexToHash("0x000000000000000000000000000000000000000000000000000000000000007b"),
		common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000000"),
	},
	Data:        hexutil.MustDecode("0x000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000400000000000000000000000000000000000000000000000000000000000000024c959c42b000000000000000000000000000000000000000000000000000000000000007b"),
	BlockNumber: 16,
	TxHash:      common.HexToHash("0xc6ff19de9299e5b290ba2d52fdb4662360ca86376613d78ee546244866a0be2d"),
	TxIndex:     74,
	BlockHash:   common.HexToHash("0x6454844075164a1d264c86d2a2c31ac1b64eb2f4ebdbbaeb4d44388fdf74470b"),
	Index:       0,
	Removed:     false,
}
var dealRawJson, _ = json.Marshal(DealLogNote)

var DealModel = deal.DealModel{
	BidId:            "123",
	ContractAddress:  shared.FlipperContractAddress,
	TransactionIndex: 74,
	Raw:              dealRawJson,
}