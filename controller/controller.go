package controller

import (
	"encoding/csv"
	"errors"
	"fmt"
	store "github.com/shimanekb/project1-C/store"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
	"path/filepath"
)

const (
	GET_COMMAND       string = "get"
	PUT_COMMAND       string = "put"
	DEL_COMMAND       string = "del"
	FIRST_LINE_RECORD string = "type"
	STORAGE_DIR       string = "storage"
	STORAGE_FILE      string = "data_records.csv"
	INDEX_FILE        string = "index_file.csv"
)

type Command struct {
	Type  string
	Key   string
	Value string
}

func ReadCsvCommands(filePath string, outputPath string) {
	csv_file, err := os.Open(filePath)

	log.Infof("Opening csv file %s", filePath)

	if err != nil {
		log.Fatalln("FATAL: Could not open csv file.", err)
	}

	log.Infof("Creating output file.")
	outErr := WriteOutputFirstLine(outputPath)
	if outErr != nil {
		log.Fatal("Could not create output file", outErr)
	}

	reader := csv.NewReader(csv_file)
	path := filepath.Join(".", STORAGE_DIR)
	err = os.MkdirAll(path, os.ModePerm)

	if err != nil {
		log.Fatalf("Cannot create directory for storage at %s", STORAGE_DIR)
	}

	logPath := filepath.Join(path, STORAGE_FILE)
	indexPath := filepath.Join(path, INDEX_FILE)
	localStore, storeErr := store.NewLocalStore(logPath, indexPath)
	if storeErr != nil {
		log.Fatal("Could not create store.", storeErr)
	}

	log.Infoln("Reading in csv records.")
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}

		if err != nil {
			log.Fatal(err)
		}

		if record[0] == FIRST_LINE_RECORD {
			log.Infoln("First line detected, skipping.")
			continue
		}
		command := Command{record[0], record[1], record[3]}
		cmd_err := ProcessCommand(command, localStore, outputPath)
		if cmd_err != nil {
			log.Errorln(cmd_err)
		}
	}

	localStore.Flush()
}

func WriteOutputFirstLine(outputPath string) error {
	file, err := os.OpenFile(outputPath, os.O_TRUNC|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	_, write_err := file.WriteString("type,key1,outcome,values\n")
	file.Close()
	return write_err
}

func WriteOutput(command Command, outcome int, value string, outputPath string) error {
	file, err := os.OpenFile(outputPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)

	if err != nil {
		return err
	}

	_, write_err := file.WriteString(fmt.Sprintf("%s,%s,%d,%s\n", command.Type,
		command.Key, outcome, value))
	file.Close()
	return write_err

}

func ProcessCommand(command Command, storage store.Store, outputPath string) error {
	switch {
	case GET_COMMAND == command.Type:
		log.Infof("Get command given for key: %s, value: %s", command.Key,
			command.Value)
		value, ok := storage.Get(command.Key)
		if ok {
			WriteOutput(command, 1, value, outputPath)
			log.Infof("Get command successful found value: %s, for key: %s",
				value, command.Key)
		} else {
			WriteOutput(command, 0, "", outputPath)
		}

		return nil
	case PUT_COMMAND == command.Type:
		log.Infof("Put command given for key: %s, value: %s", command.Key,
			command.Value)

		WriteOutput(command, 0, "", outputPath)
		return storage.Put(command.Key, command.Value)
	case DEL_COMMAND == command.Type:
		log.Infof("Del command given for key: %s, value: %s", command.Key,
			command.Value)
		storage.Del(command.Key)
		WriteOutput(command, 1, "", outputPath)

		return nil
	}

	return errors.New(fmt.Sprintf("Invalid command given: %s", command))
}
