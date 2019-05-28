package main

import (
  "os"
  "fmt"
  "mapreduce"
  "container/list"
  "strings"
  "unicode"
  "strconv"
)

// our simplified version of MapReduce does not supply a
// key to the Map function, as in the paper; only a value,
// which is a part of the input file contents
func Map(value string) *list.List {
  // KeyValue container
  kvs := list.New()

  // white-space boolean sub-routine
  split := func(char rune) bool {
    return !unicode.IsLetter(char)
  }

  // apply to split along white-space
  s := strings.FieldsFunc(value, split)

  // iterate over each word & transform to (k ,v) pair
  // f: word => (word, 1)
  for _, el := range s {
    kv := mapreduce.KeyValue{el, "1"}
    kvs.PushBack(kv)
  }

  return kvs
}

// iterate over list and add values
func Reduce(key string, values *list.List) string {
  // null count
  count := 0

  // iterate over KeyValue pairs & reduce to a sum (counts)
  for e := values.Front(); e != nil; e = e.Next() {
    s := e.Value.(string)
    i, _ := strconv.Atoi(s)
    count += i
  }

  return strconv.Itoa(count)
}

// Can be run in 3 ways:
// 1) Sequential (e.g., go run wc.go master x.txt sequential)
// 2) Master (e.g., go run wc.go master x.txt localhost:7777)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 &)
func main() {
  if len(os.Args) != 4 {
    fmt.Printf("%s: see usage comments in file\n", os.Args[0])
  } else if os.Args[1] == "master" {
    if os.Args[3] == "sequential" {
      mapreduce.RunSingle(5, 3, os.Args[2], Map, Reduce)
    } else {
      mr := mapreduce.MakeMapReduce(5, 3, os.Args[2], os.Args[3])
      // Wait until MR is done
      <- mr.DoneChannel
    }
  } else {
    mapreduce.RunWorker(os.Args[2], os.Args[3], Map, Reduce, 100)
  }
}
