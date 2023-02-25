package surfstore

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
)

// -----------------
// HELPER FUNCTIONS
// -----------------

// Function to check if it is a valid file
func CheckValidFile(fileName os.FileInfo) (validF bool) {
	if fileName.Name() == "index.db" || fileName.IsDir() {
		return false
	}
	return true
}

// Function to check and return Fatal error
func CheckFatalError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

// Function to compare if two slices of strings are equal or not
func CompareSlices(slice1, slice2 []string) bool {
	if len(slice1) != len(slice2) {
		return false
	}
	for i, str := range slice1 {
		if str != slice2[i] {
			return false
		}
	}
	return true // return true if the slices are equal
}

// -----------------------------
// LOCAL - INDEX SYNC FUNCTIONS
// -----------------------------

// Function to divide the given file into blocks
func DivideBlocks(f *os.File, blockSize int) ([]*Block, error) {
	blocksList := make([]*Block, 0)

	br := bufio.NewReader(f)
	for {
		blockBuffer := make([]byte, blockSize)
		blockRead, err := br.Read(blockBuffer)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return nil, fmt.Errorf("error reading file")
			}
		}

		blocksList = append(blocksList, &Block{
			BlockData: blockBuffer[:blockRead],
			BlockSize: int32(blockRead),
		})
	}
	return blocksList, nil
}

// Function to generate a block hash list from a given slice of blocks
func GenerateBlockHashList(blocksList []*Block) ([]string, error) {
	if len(blocksList) == 0 {
		return nil, fmt.Errorf("empty Block List received")
	}

	var blockHashList []string
	for _, block := range blocksList {
		if block.BlockSize == 0 {
			return nil, fmt.Errorf("block of size 0 received")
		}
		blockHashVal := GetBlockHashString(block.BlockData)
		blockHashList = append(blockHashList, blockHashVal)
	}
	return blockHashList, nil
}

// Function to sync the Base Directory with local index.db
func LocalSyncDB(client RPCClient) map[string]*FileMetaData {
	localFilesList, err := ioutil.ReadDir(client.BaseDir)
	CheckFatalError(err)

	metaMapDB, _ := LoadMetaFromMetaFile(client.BaseDir)
	metaMapLocal := make(map[string]*FileMetaData)

	// Check for files in local with index.db
	for _, localFileName := range localFilesList {
		if CheckValidFile(localFileName) {
			// Open the valid file
			f, err := os.Open(client.BaseDir + "/" + localFileName.Name())
			CheckFatalError(err)

			// Generate Blocks for the above local file
			blocksList, _ := DivideBlocks(f, client.BlockSize)

			// Generate local Block Hash List for the given blocks
			blockHashListLocal, _ := GenerateBlockHashList(blocksList)

			if metaDataDB, ok := metaMapDB[localFileName.Name()]; ok { // Check for files that are both in index.db and local
				if !CompareSlices(metaDataDB.BlockHashList, blockHashListLocal) {
					metaMapLocal[localFileName.Name()] = &FileMetaData{
						Filename:      localFileName.Name(),
						Version:       metaDataDB.Version + 1,
						BlockHashList: blockHashListLocal,
					}
				} else {
					metaMapLocal[localFileName.Name()] = &FileMetaData{
						Filename:      localFileName.Name(),
						Version:       metaDataDB.Version,
						BlockHashList: blockHashListLocal,
					}
				}
			} else { // Check for files that are in local but not in index.db
				metaMapLocal[localFileName.Name()] = &FileMetaData{
					Filename:      localFileName.Name(),
					Version:       1,
					BlockHashList: blockHashListLocal,
				}
			}
		}
	}

	// Check for files deleted in local but present in index.db and sync in metaMapLocal
	for _, m := range metaMapDB {
		if _, ok := metaMapLocal[m.Filename]; !ok {
			var version int32
			if len(metaMapDB[m.Filename].BlockHashList) == 1 && metaMapDB[m.Filename].BlockHashList[0] == "0" {
				version = m.Version
			} else {
				version = m.Version + 1
			}
			metaMapLocal[m.Filename] = &FileMetaData{
				Filename:      m.Filename,
				Version:       version,
				BlockHashList: []string{"0"},
			}
		}
	}
	return metaMapLocal
}

// ------------------------------
// SERVER - LOCAL SYNC FUNCTIONS
// ------------------------------

func writeBlockstoLocal(fileInfoMapLocal map[string]*FileMetaData, serverMeta *FileMetaData) map[string]*FileMetaData {
	fileInfoMapLocal[serverMeta.Filename] = &FileMetaData{
		Filename:      serverMeta.Filename,
		Version:       serverMeta.Version,
		BlockHashList: serverMeta.BlockHashList,
	}
	return fileInfoMapLocal
}

func downloadBlocks(client RPCClient, m *FileMetaData) {
	f, err := os.Create(client.BaseDir + "/" + m.Filename)
	CheckFatalError(err)
	defer f.Close()

	// File has been in deleted in server.
	if len(m.BlockHashList) == 1 && m.BlockHashList[0] == "0" {
		if err := os.Remove(client.BaseDir + "/" + m.Filename); err != nil {
			log.Println("Could not remove local file: ", err)
			return
		}
		return
	}
	var data = ""
	var blockStoreAddr string
	err2 := client.GetBlockStoreAddr(&blockStoreAddr)
	if err2 != nil {
		fmt.Println(err2.Error())
	}

	for _, hashVal := range m.BlockHashList {
		if hashVal == "0" {
			fmt.Println("That is a deleted file")
			return
		}
		var block Block
		err2 := client.GetBlock(hashVal, blockStoreAddr, &block)
		if err2 != nil {
			fmt.Println(err2.Error())
		}
		data += string(block.BlockData)
	}

	_, err3 := f.WriteString(data)
	CheckFatalError(err3)
}

func uploadBlocks(client RPCClient, metaLocal *FileMetaData) {
	if _, err := os.Stat(client.BaseDir + "/" + metaLocal.Filename); errors.Is(err, os.ErrNotExist) {
		err = client.UpdateFile(metaLocal, &metaLocal.Version)
		if err != nil {
			log.Println("file could not be uploaded")
		}
		return
	}
	f, err := os.Open(client.BaseDir + "/" + metaLocal.Filename)
	CheckFatalError(err)

	// Generate Blocks for the above local file
	blocksList, _ := DivideBlocks(f, client.BlockSize)

	var blockStoreAddr string
	err2 := client.GetBlockStoreAddr(&blockStoreAddr)
	if err2 != nil {
		fmt.Println(err2.Error())
	}

	for _, block := range blocksList {
		var succ bool
		// Get block store address for each block

		// Put the block on Server
		err2 := client.PutBlock(block, blockStoreAddr, &succ)
		if err2 != nil {
			fmt.Println(err2.Error())
		}

		err3 := client.UpdateFile(metaLocal, &metaLocal.Version)
		if err3 != nil {
			fmt.Println(err3.Error())
		}
	}
}

func ServerSync(client RPCClient, fileInfoMapLocal map[string]*FileMetaData) (map[string]*FileMetaData, error) {
	fileInfoMapServer := make(map[string]*FileMetaData)
	client.GetFileInfoMap(&fileInfoMapServer)

	for _, metaServer := range fileInfoMapServer {
		if metaLocal, ok := fileInfoMapLocal[metaServer.Filename]; ok { // Check for versions if file is present in local and server
			if metaServer.Version >= metaLocal.Version {
				downloadBlocks(client, metaServer)
				fileInfoMapLocal = writeBlockstoLocal(fileInfoMapLocal, metaServer)
			}
		} else { // Download the blocks if in Server but not present in local
			downloadBlocks(client, metaServer)
			fileInfoMapLocal = writeBlockstoLocal(fileInfoMapLocal, metaServer)
		}
	}

	for _, metaLocal := range fileInfoMapLocal {
		if metaServer, ok := fileInfoMapServer[metaLocal.Filename]; ok { // Check for versions if file is present in local and server
			if metaServer.Version < metaLocal.Version {
				uploadBlocks(client, metaLocal)
			}
		} else { // Upload the blocks if in Local but not present in server
			uploadBlocks(client, metaLocal)
		}
	}
	return fileInfoMapLocal, nil
}

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	// Sync Local Base Directory to index.db
	fileInfoMapLocal := LocalSyncDB(client)

	// Sync Local Meta to Server Meta
	fileInfoMapLocal, err := ServerSync(client, fileInfoMapLocal)
	if err != nil {
		fmt.Println(err.Error())
	}
	err2 := WriteMetaFile(fileInfoMapLocal, client.BaseDir)
	if err2 != nil {
		fmt.Println(err2.Error())
	}
}
