package random

import (
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

var randomWords = []string{
	"apple",
	"orange",
	"banana",
	"pear",
	"grape",
	"pineapple",
	"mango",
	"strawberry",
	"raspberry",
	"blueberry",
}

// Init must be called at the start of every program.
//
// If CODECRAFTERS_RANDOM_SEED is set, it will be used to generate predictable random numbers.
func Init() {
	if seed := os.Getenv("CODECRAFTERS_RANDOM_SEED"); seed != "" {
		seedInt, err := strconv.Atoi(seed)
		if err != nil {
			panic(err)
		}

		rand.Seed(int64(seedInt))
	} else {
		rand.Seed(time.Now().UnixNano())
	}
}

// RandomInt returns a random integer between min and max.
func RandomInt(min, max int) int {
	return rand.Intn(max-min) + min
}

// RandomWord returns a random word from the list of words.
func RandomWord() string {
	return randomWords[rand.Intn(len(randomWords))]
}

// RandomWords returns a random list of n words.
func RandomWords(n int) []string {
	return RandomElementsFromArray(randomWords, n)
}

// RandomString returns a random string of 6 words.
func RandomString() string {
	return strings.Join(RandomWords(6), " ")
}

// RandomStrings returns a random list of n strings.
func RandomStrings(n int) []string {
	l := make([]string, n)

	for i := range l {
		l[i] = RandomString()
	}

	return l
}

func RandomElementFromArray[T any](arr []T) T {
	return RandomElementsFromArray(arr, 1)[0]
}

func RandomElementsFromArray[T any](arr []T, count int) []T {
	// Randomly selects `count` unique elements from the given array
	// and returns them in a new array.
	for count > len(arr) {
		// If we need more elements than the array has, we'll just append the array to itself repeatedly.
		arr = append(arr, arr...)
	}
	elements := make([]T, count)
	for i, randIndex := range rand.Perm(len(arr))[:count] {
		elements[i] = arr[randIndex]
	}

	return elements
}
