package main

import (
	"fmt"
	"os"
)

const (
	DEFAULT_ADDR      = ":8111"
	DEFAULT_HTTP_ADDR = ":8122"
)

func main() {
	fmt.Fprintf(os.Stdout, "Start server at: [%s]\n", DEFAULT_ADDR)
	fmt.Fprintf(os.Stdout, "Start http server at: [%s]\n", DEFAULT_HTTP_ADDR)
	StartServer(DEFAULT_ADDR, DEFAULT_HTTP_ADDR)
}
