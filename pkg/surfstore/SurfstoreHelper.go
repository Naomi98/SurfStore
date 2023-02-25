package surfstore

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

/* Hash Related */
func GetBlockHashBytes(blockData []byte) []byte {
	h := sha256.New()
	h.Write(blockData)
	return h.Sum(nil)
}

func GetBlockHashString(blockData []byte) string {
	blockHash := GetBlockHashBytes(blockData)
	return hex.EncodeToString(blockHash)
}

/* File Path Related */
func ConcatPath(baseDir, fileDir string) string {
	return baseDir + "/" + fileDir
}

/*
	Writing Local Metadata File Related
*/

const createTable string = `create table if not exists indexes (
		fileName TEXT, 
		version INT,
		hashIndex INT,
		hashValue TEXT
	);`

const insertTuple string = `INSERT INTO INDEXES (
		fileName,
		version,
		hashIndex,
		hashValue
	) VALUES (?, ?, ?, ?);`

// WriteMetaFile writes the file meta map back to local metadata file index.db
func WriteMetaFile(fileMetas map[string]*FileMetaData, baseDir string) error {
	// remove index.db file if it exists
	outputMetaPath := ConcatPath(baseDir, DEFAULT_META_FILENAME)
	if _, err := os.Stat(outputMetaPath); err == nil {
		e := os.Remove(outputMetaPath)
		if e != nil {
			log.Fatal("Error During Meta Write Back")
		}
	}
	db, err := sql.Open("sqlite3", outputMetaPath)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	statement, err := db.Prepare(createTable)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	statement.Exec()

	for _, m := range fileMetas {
		for hIdx, hVal := range m.BlockHashList {
			statement, err = db.Prepare(insertTuple)
			if err != nil {
				log.Fatal("Error During Meta Write Back")
			}
			statement.Exec(m.Filename, m.Version, hIdx, hVal)
		}
	}
	return nil
}

/*
Reading Local Metadata File Related
*/
const getDistinctFileName string = `SELECT DISTINCT fileName, version FROM INDEXES;`

const getTuplesByFileName string = `SELECT hashIndex, hashValue FROM INDEXES WHERE FILENAME = ? ORDER BY hashIndex;`

// LoadMetaFromMetaFile loads the local metadata file into a file meta map.
// The key is the file's name and the value is the file's metadata.
// You can use this function to load the index.db file in this project.
func LoadMetaFromMetaFile(baseDir string) (fileMetaMap map[string]*FileMetaData, e error) {
	metaFilePath, _ := filepath.Abs(ConcatPath(baseDir, DEFAULT_META_FILENAME))
	fileMetaMap = make(map[string]*FileMetaData)
	metaFileStats, e := os.Stat(metaFilePath)
	if e != nil || metaFileStats.IsDir() {
		return fileMetaMap, nil
	}
	db, err := sql.Open("sqlite3", metaFilePath)
	if err != nil {
		log.Fatal("Error When Opening Meta")
	}

	filenames, err := db.Query(getDistinctFileName)
	if err != nil {
		return nil, fmt.Errorf("error getting file names")
	}
	defer filenames.Close()
	for filenames.Next() {
		var fileName string
		var version int32
		if err := filenames.Scan(&fileName, &version); err != nil {
			return nil, fmt.Errorf("error scanning filenames")
		}

		tuples, err2 := db.Query(getTuplesByFileName, fileName)
		if err2 != nil {
			return nil, fmt.Errorf("error getting tuples")
		}
		var blockHashList []string
		for tuples.Next() {
			var hashIndex int
			var hashValue string
			if err := tuples.Scan(&hashIndex, &hashValue); err != nil {
				return nil, fmt.Errorf("error scanning tuples")
			}
			blockHashList = append(blockHashList, hashValue)
		}
		fileMetaData := new(FileMetaData)
		fileMetaData.Filename = fileName
		fileMetaData.Version = version
		fileMetaData.BlockHashList = blockHashList
		fileMetaMap[fileName] = fileMetaData
	}
	return fileMetaMap, nil
}

/*
	Debugging Related
*/

// PrintMetaMap prints the contents of the metadata map.
// You might find this function useful for debugging.
func PrintMetaMap(metaMap map[string]*FileMetaData) {

	fmt.Println("--------BEGIN PRINT MAP--------")

	for _, filemeta := range metaMap {
		fmt.Println("\t", filemeta.Filename, filemeta.Version)
		for _, blockHash := range filemeta.BlockHashList {
			fmt.Println("\t", blockHash)
		}
	}

	fmt.Println("---------END PRINT MAP--------")

}
