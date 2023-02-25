package surfstore

import (
	context "context"
	"fmt"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap    map[string]*FileMetaData
	BlockStoreAddr string
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	if m.FileMetaMap == nil {
		return nil, fmt.Errorf("file meta map is empty")
	} else {
		fileInfoMap := &FileInfoMap{
			FileInfoMap: m.FileMetaMap,
		}
		return fileInfoMap, nil
	}
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	fName := fileMetaData.Filename

	if _, ok := m.FileMetaMap[fName]; ok {
		if fileMetaData.Version == m.FileMetaMap[fName].Version+1 {
			m.FileMetaMap[fName] = fileMetaData
			return &Version{Version: fileMetaData.Version}, nil
		} else {
			return &Version{Version: -1}, fmt.Errorf("the version being tried to store is incorrect")
		}
	} else {
		m.FileMetaMap[fName] = fileMetaData
		return &Version{Version: fileMetaData.Version}, nil
	}
}

func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
	if m.BlockStoreAddr == "" {
		return nil, fmt.Errorf("no blockStoreAddress found")
	} else {
		blockAddr := &BlockStoreAddr{
			Addr: m.BlockStoreAddr,
		}
		return blockAddr, nil
	}
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddr string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddr: blockStoreAddr,
	}
}
