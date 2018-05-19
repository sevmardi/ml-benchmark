package main

import (
	"bufio"
	"io/ioutil"
	"log"
	"os"
)

func main() {
	//read the folder
	files, err := ioutil.ReadDir("training_set")
	if err != nil {
		log.Fatal(err)
		panic(err)
	}
	// save the output to a file
	fout, err := os.Create("netflix_training_merged")
	if err != nil {
		log.Fatal(err)
		panic(err)
	}

	defer fout.Close()
	//iterate over every file in the folder..
	for _, fileName := range files {
		file, err := os.Open("training_set/" + fileName.Name())
		if err != nil {
			log.Fatal(err)
			panic(err)
		}

		first := true
		movie := 0
		scanner := bufio.NewScanner(file)
		var mvNr string
		//..scan the folder
		for scanner.Scan() {
			if first && movie == 0 {
				mvNr = scanner.Text()
				mvNr = mvNr[0 : len(mvNr)-1]

			} else {
				line := scanner.Text()
				fout.WriteString(mvNr + ", " + line + "\n")
			}
			first = false
			movie = 1
		}
		if err := scanner.Err(); err != nil {
			log.Fatal(err)
			panic(err)
		}

		file.Close()
	}
}
