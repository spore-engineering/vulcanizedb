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
	"errors"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/statediff"
	"math/big"
	"math/rand"
)

var (
	BlockNumber     = big.NewInt(rand.Int63())
	BlockHash       = "0xfa40fbe2d98d98b3363a778d52f2bcd29d6790b9b3f3cab2b167fd12d3550f73"
	CodeHash        = common.Hex2Bytes("0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470")
	NewNonceValue   = rand.Uint64()
	NewBalanceValue = rand.Int63()
	ContractRoot    = common.HexToHash("0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")
	StoragePath     = common.HexToHash("0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470").Bytes()
	StorageKey      = common.HexToHash("0000000000000000000000000000000000000000000000000000000000000001").Bytes()
	StorageValue    = common.Hex2Bytes("0x03")
	storage         = []statediff.StorageDiff{{
		Key:   StorageKey,
		Value: StorageValue,
		Path:  StoragePath,
		Proof: [][]byte{},
	}}
	emptyStorage           = make([]statediff.StorageDiff, 0)
	contractAddress        = common.HexToAddress("0xaE9BEa628c4Ce503DcFD7E305CaB4e29E7476592")
	ContractLeafKey        = crypto.Keccak256Hash(contractAddress[:])
	anotherContractAddress = common.HexToAddress("0xaE9BEa628c4Ce503DcFD7E305CaB4e29E7476593")
	AnotherContractLeafKey = crypto.Keccak256Hash(anotherContractAddress[:])

	testAccount = state.Account{
		Nonce:    NewNonceValue,
		Balance:  big.NewInt(NewBalanceValue),
		Root:     ContractRoot,
		CodeHash: CodeHash,
	}
	valueBytes, _       = rlp.EncodeToBytes(testAccount)
	CreatedAccountDiffs = []statediff.AccountDiff{
		{
			Key:     ContractLeafKey.Bytes(),
			Value:   valueBytes,
			Storage: storage,
		},
		{
			Key:     AnotherContractLeafKey.Bytes(),
			Value:   valueBytes,
			Storage: emptyStorage,
		},
	}

	UpdatedAccountDiffs = []statediff.AccountDiff{{
		Key:     AnotherContractLeafKey.Bytes(),
		Value:   valueBytes,
		Storage: storage,
	}}

	DeletedAccountDiffs = []statediff.AccountDiff{{
		Key:     ContractLeafKey.Bytes(),
		Value:   valueBytes,
		Storage: storage,
	}}

	MockStateDiff = statediff.StateDiff{
		BlockNumber:     BlockNumber,
		BlockHash:       common.HexToHash(BlockHash),
		CreatedAccounts: CreatedAccountDiffs,
		DeletedAccounts: DeletedAccountDiffs,
		UpdatedAccounts: UpdatedAccountDiffs,
	}
	MockStateDiffBytes, _ = rlp.EncodeToBytes(MockStateDiff)

	mockTransaction1 = types.NewTransaction(0, common.HexToAddress("0x0"), big.NewInt(1000), 50, big.NewInt(100), nil)
	mockTransaction2 = types.NewTransaction(1, common.HexToAddress("0x1"), big.NewInt(2000), 100, big.NewInt(200), nil)
	MockTransactions = types.Transactions{mockTransaction1, mockTransaction2}

	mockReceipt1 = types.NewReceipt(common.HexToHash("0x0").Bytes(), false, 50)
	mockReceipt2 = types.NewReceipt(common.HexToHash("0x1").Bytes(), false, 100)
	MockReceipts = types.Receipts{mockReceipt1, mockReceipt2}

	MockHeader = types.Header{
		Time:        0,
		Number:      BlockNumber,
		Root:        common.HexToHash("0x0"),
		TxHash:      common.HexToHash("0x0"),
		ReceiptHash: common.HexToHash("0x0"),
	}
	MockBlock       = types.NewBlock(&MockHeader, MockTransactions, nil, MockReceipts)
	MockBlockRlp, _ = rlp.EncodeToBytes(MockBlock)

	MockStatediffPayload = statediff.Payload{
		BlockRlp:     MockBlockRlp,
		StateDiffRlp: MockStateDiffBytes,
		Err:          nil,
	}

	EmptyStatediffPayload = statediff.Payload{
		BlockRlp:     []byte{},
		StateDiffRlp: []byte{},
		Err:          nil,
	}

	ErrStatediffPayload = statediff.Payload{
		BlockRlp:     []byte{},
		StateDiffRlp: []byte{},
		Err:          errors.New("mock error"),
	}
)