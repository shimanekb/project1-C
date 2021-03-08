package main

import (
	"flag"
	"github.com/shimanekb/project1-C/controller"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"os"
)

func main() {
	var logFlag *bool = flag.Bool("logs", false, "Enable logs")
	flag.Parse()

	if *logFlag {
		file, _ := os.OpenFile("logs.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY,
			0666)
		log.SetOutput(file)
	} else {
		log.SetOutput(ioutil.Discard)
	}

	args := flag.Args()
	if flag.NArg() < 2 {
		log.Fatalln("Missing file path argument for input.")
	}

	filePath := args[0]
	outputPath := args[1]
	controller.ReadCsvCommands(filePath, outputPath)
}
