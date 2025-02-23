package main

import (
	"bufio"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync"
)

type MapOut struct {
	Uniques map[int]bool
	Max     int
	Sum     int
	Count   int
}

type ReduceOut struct {
	UniqueNumbers []int
	Max           int
	Sum           int
	Count         int
	UniqueCount   int
	Mean          float64
}

func mapper(numbers []int, out chan<- MapOut, wg *sync.WaitGroup) {
	defer wg.Done()
	mapOut := MapOut{Max: numbers[0], Uniques: make(map[int]bool)}
	for _, num := range numbers {
		if num > mapOut.Max {
			mapOut.Max = num
		}
		mapOut.Sum += num
		mapOut.Count++
		mapOut.Uniques[num] = true
	}
	out <- mapOut
}

func reducer(in <-chan MapOut, done chan<- ReduceOut) {
	out := ReduceOut{Max: -1}
	unique := make(map[int]bool)
	for res := range in {
		if res.Max > out.Max {
			out.Max = res.Max
		}
		out.Sum += res.Sum
		out.Count += res.Count
		for num := range res.Uniques {
			unique[num] = true
		}
	}
	out.UniqueCount = len(unique)
	for num := range unique {
		out.UniqueNumbers = append(out.UniqueNumbers, num)
	}
	out.Mean = float64(out.Sum) / float64(out.Count)
	done <- out
}

func main() {
	if len(os.Args) < 2 {
		slog.Error("File name not passed")
		return
	}
	fileName := os.Args[1]

	file, err := os.Open(fileName)
	if err != nil {
		slog.Error("Error opening file", "error", err)
		return
	}
	defer file.Close()

	var numbers []int
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		num, err := strconv.Atoi(scanner.Text())
		if err == nil {
			numbers = append(numbers, num)
		}
	}

	if len(numbers) == 0 {
		slog.Error("No numbers found in file")
		return
	}

	chunkSize := len(numbers) / 4
	if chunkSize == 0 {
		chunkSize = len(numbers)
	}

	var wg sync.WaitGroup
	mapRes := make(chan MapOut, 4)
	reduceRes := make(chan ReduceOut, 1)

	for i := 0; i < len(numbers); i += chunkSize {
		end := i + chunkSize
		if end > len(numbers) {
			end = len(numbers)
		}
		wg.Add(1)
		go mapper(numbers[i:end], mapRes, &wg)
	}

	go func() {
		wg.Wait()
		close(mapRes)
	}()

	go reducer(mapRes, reduceRes)

	out := <-reduceRes

	fileOut, err := os.Create("unique_numbers.txt")
	if err != nil {
		slog.Error("Error creating file", "error", err)
		return
	}
	defer file.Close()

	for _, num := range out.UniqueNumbers {
		_, err := fmt.Fprintf(fileOut, "%d\n", num)
		if err != nil {
			slog.Error("Error while writing to file", "error", err)
			return
		}
	}

	fmt.Println("(a) the greatest number", out.Max)
	fmt.Println("(b) the arithmetic mean of all numbers", out.Mean)
	fmt.Println("(c) the set of the same numbers, from which duplicates are excluded, is written to the file 'unique_numbers.txt'")
	fmt.Println("(d) the number of distinct integers in the input set", out.UniqueCount)
}
