package main

import (
	"CraneFrontEnd/internal/calloc"
	"log"
)

func main() {
	parser := calloc.CmdArgParser()
	err := parser.Execute()
	if err != nil {
		log.Fatal(err)
	}
}
