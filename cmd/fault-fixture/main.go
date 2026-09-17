package main

import (
	"flag"
	"fmt"

	"durable-agent-execution-engine/internal/faults"
)

func main() {
	seed := flag.Int("seed", 1, "fixture seed")
	boundary := flag.String("boundary", "after-effect", "named fault boundary")
	flag.Parse()

	client, err := faults.FromEnvironment()
	if err != nil {
		panic(err)
	}
	if client == nil {
		fmt.Printf("fault fixture seed=%d boundary=%s\n", *seed, *boundary)
		return
	}
	defer client.Close()
	fixtureValue := (*seed*1103515245 + 12345) & 0x7fffffff
	fields := map[string]any{
		"workflow_id":   fmt.Sprintf("go-fixture-%08x", *seed),
		"fixture_value": fixtureValue,
	}
	if err := client.Hit(*boundary, fields); err != nil {
		panic(err)
	}
	if err := client.Emit("released", *boundary, fields); err != nil {
		panic(err)
	}
	fmt.Printf("fault fixture released seed=%d\n", *seed)
}
