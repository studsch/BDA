package main

import (
	"fmt"
	"log/slog"
	"math/rand"
	"os"
)

const (
	fileName = "very_large_integer_file.txt"
	numCount = 10_000
	maxNum   = 1_000
)

func main() {
	file, err := os.Create(fileName)
	if err != nil {
		slog.Error("Error creating file", "error", err)
		return
	}
	defer file.Close()

	for i := 0; i < numCount; i++ {
		_, err := fmt.Fprintf(file, "%d\n", rand.Intn(maxNum))
		if err != nil {
			slog.Error("Error while writing to file", "error", err)
			return
		}
	}

	slog.Info("File created successfully", "file", fileName)
}
