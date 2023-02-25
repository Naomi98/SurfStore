package surfstore

import (
	context "context"
	"fmt"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	retrievedBlock := bs.BlockMap[blockHash.Hash]
	if retrievedBlock != nil {
		return &Block{
			BlockData: retrievedBlock.BlockData,
			BlockSize: retrievedBlock.BlockSize,
		}, nil
	}
	return &Block{}, fmt.Errorf("no such hash value found")
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	if block != nil {
		blockKey := GetBlockHashString(block.BlockData)
		bs.BlockMap[blockKey] = block
		return &Success{Flag: true}, nil
	}
	return &Success{Flag: false}, fmt.Errorf("no Block found")
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	blockHashesOut := &BlockHashes{}
	for _, keyIn := range blockHashesIn.Hashes {
		if _, ok := bs.BlockMap[keyIn]; ok {
			blockHashesOut.Hashes = append(blockHashesOut.Hashes, keyIn)
		}
	}
	if len(blockHashesOut.Hashes) == 0 {
		return nil, fmt.Errorf("no subset found")
	} else {
		return blockHashesOut, nil
	}
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
