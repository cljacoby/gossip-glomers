package main

import (
    "encoding/json"
    "fmt"
    "os"
    "log"
    "strconv"
    "strings"
    maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

 /*
  * Goal is to generate globally unique IDs from each node.
  * 
  * Simple (sort-of-hack) way to do it will be to combine two uint32's
  * into a single uint64. The first uint32 comprising the left bits will
  * be the node ID. The second uint32 comprising the right bits will be a counter,
  * incremented in each node's local execution state.
  * 
  * Basically we prefix a locally unique ID with a node-specific prefix to get a globally
  * unique ID. This will fail if the number of messages exceeds 4_294_967_296 on any node,
  * which seems unlikely. It would also fail if the number of nodes exceeds
  * 4_294_967_296, which seems even more unlikely.
  *
  */

count uint32 := 0

func main() {
    n := maelstrom.NewNode()
    n.Handle("generate", func(msg maelstrom.Message) error {
        fmt.Fprintln(os.Stderr, msg.Body)

        // Unmarshal the message body as an loosely-typed map.
        var body map[string]any
        if err := json.Unmarshal(msg.Body, &body); err != nil {
            return err
        }

        x := strings.TrimPrefix(n.ID(), "n");
        left, err := strconv.ParseUint(x, 10, 64)
        if err != nil {
            log.Fatal("Failed to parse id to u32, x=%s", x);
        }

        right := count;
        count = count + 1;

        id := left << 32 | uint64(right);
        log.Println("id = %d", id);

        // Update the message type to return back.
        body["type"] = "generate_ok"
        body["id"] = id

        // Echo the original message back with the updated message type.
        return n.Reply(msg, body)
    })

    // Begin accepting message on stdin
    if err := n.Run(); err != nil {
        log.Fatal(err)
    }

}
