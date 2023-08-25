package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ejuju/tridb/pkg/tridb"
)

func main() {
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)

	if len(os.Args) <= 1 {
		fmt.Println("missing database file path")
		return
	}

	start := time.Now()
	f, err := tridb.Open(os.Args[1])
	if err != nil {
		log.Println(err)
		return
	}
	defer f.Close()

	fmt.Printf("Loaded %q in %s\nType a command and press enter: ", f.Path(), time.Since(start))

	go func() {
		bufs := bufio.NewScanner(os.Stdin)
		for bufs.Scan() {
			handleCommand(f, bufs.Text())
			fmt.Print("\n? ")
		}
		if err := bufs.Err(); err != nil {
			panic(err)
		}
	}()

	<-interrupt
	err = f.Close()
	if err != nil {
		log.Println(err)
		return
	}
	log.Println("goodbye!")
}

func handleCommand(f *tridb.File, line string) {
	parts := strings.Split(line, " ")
	keyword := parts[0]

	// Find and exec command
	for _, cmd := range commands {
		isMatch := false
		for _, kw := range cmd.keywords {
			if kw == keyword {
				isMatch = true
			}
		}
		if isMatch {
			var args []string
			if len(cmd.args) > 0 {
				if len(parts)-1 != len(cmd.args) {
					fmt.Printf("%q needs %d argument(s): %s\n", keyword, len(cmd.args), strings.Join(cmd.args, ", "))
					return
				}
				args = parts[1:]
			}
			cmd.do(f, args...)
			return
		}
	}

	fmt.Printf("\nCommand not found: %q\n", keyword)
	printAvailableCommands(commands)
}

func printAvailableCommands(commands []*command) {
	fmt.Println("Available commands:")
	for _, cmd := range commands {
		fmt.Printf("> \033[033m%-15s\033[0m \033[2m%s\033[0m\n", cmd.keywords[0], cmd.desc)
	}
}

type command struct {
	desc     string
	keywords []string
	args     []string
	do       func(f *tridb.File, args ...string)
}

var commands = []*command{
	{
		keywords: []string{"compact"},
		desc:     "removes deleted key-value pairs and re-writes rows in lexicographical order",
		do: func(f *tridb.File, args ...string) {
			start := time.Now()
			err := f.Compact()
			if err != nil {
				fmt.Println(err)
				return
			}
			fmt.Printf("compacted in %s\n", time.Since(start))
		},
	},
	{
		keywords: []string{"set", "+"},
		desc:     "set a key-value pair in the database",
		args:     []string{"key", "value"},
		do: func(f *tridb.File, args ...string) {
			key, value := []byte(args[0]), []byte(args[1])
			err := f.ReadWrite(func(r *tridb.Reader, w *tridb.Writer) error {
				w.Set(key, value)
				return nil
			})
			if err != nil {
				fmt.Println(err)
				return
			}
			fmt.Printf("%q is now %q\n", key, value)
		},
	},
	{
		keywords: []string{"delete", "-"},
		desc:     "delete a key-value pair from the database",
		args:     []string{"key"},
		do: func(f *tridb.File, args ...string) {
			key := []byte(args[0])
			err := f.ReadWrite(func(r *tridb.Reader, w *tridb.Writer) error {
				w.Delete(key)
				return nil
			})
			if err != nil {
				fmt.Println(err)
				return
			}
			fmt.Printf("deleted %q\n", key)
		},
	},
	{
		keywords: []string{"get"},
		desc:     "get the value associated with a given key",
		args:     []string{"key"},
		do: func(f *tridb.File, args ...string) {
			key := []byte(args[0])
			_ = f.Read(func(r *tridb.Reader) error {
				value, err := r.Get(key)
				if err != nil {
					fmt.Println(err)
					return nil
				}
				if value == nil {
					fmt.Printf("%q not found\n", key)
					return nil
				}
				fmt.Printf("%q = %q\n", key, value)
				return nil
			})
		},
	},
	{
		keywords: []string{"has"},
		desc:     "reports whether a key exists",
		args:     []string{"key"},
		do: func(f *tridb.File, args ...string) {
			key := []byte(args[0])
			_ = f.Read(func(r *tridb.Reader) error {
				fmt.Println(r.Has(key))
				return nil
			})
		},
	},
	{
		keywords: []string{"count"},
		desc:     "reports the number of unique keys",
		do: func(f *tridb.File, args ...string) {
			_ = f.Read(func(r *tridb.Reader) error {
				fmt.Println(r.Count())
				return nil
			})
		},
	},
	{
		keywords: []string{"count-prefix"},
		desc:     "reports the number of unique keys with the given prefix",
		args:     []string{"prefix"},
		do: func(f *tridb.File, args ...string) {
			prefix := []byte(args[0])
			_ = f.Read(func(r *tridb.Reader) error {
				fmt.Println(r.CountPrefix(prefix))
				return nil
			})
		},
	},
	{
		keywords: []string{"all"},
		desc:     "show all unique keys",
		do: func(f *tridb.File, args ...string) {
			_ = f.Read(func(r *tridb.Reader) error {
				return r.Walk(nil, func(key []byte) error {
					fmt.Printf("%q\n", key)
					return nil
				})
			})
		},
	},
	{
		keywords: []string{"all-prefix"},
		desc:     "show all unique keys",
		args:     []string{"prefix"},
		do: func(f *tridb.File, args ...string) {
			prefix := []byte(args[0])
			_ = f.Read(func(r *tridb.Reader) error {
				return r.Walk(&tridb.WalkOptions{Prefix: prefix}, func(key []byte) error {
					fmt.Printf("%q\n", key)
					return nil
				})
			})
		},
	},
	{
		keywords: []string{"fill"},
		desc:     "fill the database with the given number of key-value pairs",
		args:     []string{"number"},
		do: func(f *tridb.File, args ...string) {
			start := time.Now()
			num, err := strconv.Atoi(args[0])
			if err != nil {
				fmt.Println(err)
				return
			}

			err = f.ReadWrite(func(r *tridb.Reader, w *tridb.Writer) error {
				for i := 0; i < num; i++ {
					key := []byte(strconv.Itoa(i))
					w.Set(key, []byte(time.Now().Format(time.RFC3339)))
				}
				return nil
			})
			if err != nil {
				fmt.Println(err)
				return
			}
			elapsed := time.Since(start)
			fmt.Printf("added %d rows in %s (%.f rows per second)\n", num, elapsed, float64(num)/elapsed.Seconds())
		},
	},
}
