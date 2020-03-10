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

package postgres

import (
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq" //postgres driver
	"github.com/makerdao/vulcanizedb/pkg/config"
	"github.com/makerdao/vulcanizedb/pkg/core"
)

type DB struct {
	*sqlx.DB
	Node   core.Node
	NodeID int64
}

func NewDB(databaseConfig config.Database, node core.Node) (*DB, error) {
	connectString := config.DbConnectionString(databaseConfig)
	db, connectErr := sqlx.Connect("postgres", connectString)
	if connectErr != nil {
		return &DB{}, ErrDBConnectionFailed(connectErr)
	}
	pg := DB{DB: db, Node: node}
	nodeErr := pg.CreateNode(&node)
	if nodeErr != nil {
		return &DB{}, ErrUnableToSetNode(nodeErr)
	}
	return &pg, nil
}

func (db *DB) CreateNode(node *core.Node) error {
	var nodeID int64
	err := db.QueryRow(
		`WITH nodeID AS (
			INSERT INTO eth_nodes (genesis_block, network_id, eth_node_id, client_name)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT ON CONSTRAINT eth_nodes_genesis_block_network_id_eth_node_id_client_name_key DO NOTHING
			RETURNING id
		)
		SELECT id FROM eth_nodes WHERE genesis_block = $1 AND network_id = $2 AND eth_node_id = $3 AND client_name = $4
		UNION
		SELECT id FROM nodeID`,
		node.GenesisBlock, node.NetworkID, node.ID, node.ClientName).Scan(&nodeID)
	if err != nil {
		return ErrUnableToSetNode(err)
	}
	db.NodeID = nodeID
	return nil
}
