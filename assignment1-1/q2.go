package cos418_hw1_1

import (
	"bufio"
	"io"
	"os"
	"strconv"
)

// test comment

// Sum numbers from channel `nums` and output sum to `out`.
// You should only output to `out` once.
// Do NOT modify function signature.
func sumWorker(nums chan int, out chan int) {
	// TODO: implement me
	// HINT: use for loop over `nums`
	sum := 0
	for n := range nums {
		sum += n
	}
	out <- sum
}

// Read integers from the file `fileName` and return sum of all values.
// This function must launch `num` go routines running
// `sumWorker` to find the sum of the values concurrently.
// You should use `checkError` to handle potential errors.
// Do NOT modify function signature.
func sum(numWorkers int, fileName string) int {
	// TODO: implement me
	// HINT: use `readInts` and `sumWorkers`
	// HINT: used buffered channels for splitting numbers between workers
	r, err := os.Open(fileName)
	checkError(err)
	inputs, err := readInts(r)
	checkError(err)

	bufsize := int(len(inputs) / numWorkers)
	//TODO handle case where bufsize is non-integer

	// Sizing output buffer = number of worker threads so that workers don't have to wait to output (Unnecessary?)
	out := make(chan int, numWorkers)
	for i := 0; i < numWorkers; i++ {
		input := make(chan int, bufsize)
		go sumWorker(input, out)
		for _, n := range inputs[i*bufsize : (i+1)*bufsize] {
			input <- n
		}
		close(input)
	}

	sum := 0
	// Collecting partial sums
	for i := 0; i < numWorkers; i++ {
		sum += <-out
	}
	return sum
}

// Read a list of integers separated by whitespace from `r`.
// Return the integers successfully read with no error, or
// an empty slice of integers and the error that occurred.
// Do NOT modify this function.
func readInts(r io.Reader) ([]int, error) {
	scanner := bufio.NewScanner(r)
	scanner.Split(bufio.ScanWords)
	var elems []int
	for scanner.Scan() {
		val, err := strconv.Atoi(scanner.Text())
		if err != nil {
			return elems, err
		}
		elems = append(elems, val)
	}
	return elems, nil
}
