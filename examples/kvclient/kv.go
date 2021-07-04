// Command kvclient implements a client for a key-value store.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/rfratto/croissant/examples/kv/kvproto"
	"github.com/rfratto/croissant/id"
	"github.com/rfratto/croissant/node"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

func main() {
	var (
		serverAddr string
	)

	cmd := &cobra.Command{Use: "kvclient"}
	cmd.PersistentFlags().StringVarP(&serverAddr, "server-addr", "s", "", "server to connect to (required)")

	keyGen := id.NewGenerator(32)

	getCmd := &cobra.Command{
		Use:  "get [key]",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if serverAddr == "" {
				return fmt.Errorf("--server-addr not set")
			}

			cc, err := grpc.Dial(serverAddr, grpc.WithInsecure())
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to connect to server: %s", err)
				return nil
			}

			cli := kvproto.NewKVClient(cc)
			val, err := cli.Get(
				node.WithClientKey(context.Background(), keyGen.Get(args[0])),
				&kvproto.GetRequest{Key: args[0]},
			)
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to get key: %s", err)
				return nil
			}

			fmt.Println(val.Value)
			return nil
		},
	}

	setCmd := &cobra.Command{
		Use:  "set [key] [value]",
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			if serverAddr == "" {
				return fmt.Errorf("--server-addr not set")
			}

			cc, err := grpc.Dial(serverAddr, grpc.WithInsecure())
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to connect to server: %s", err)
				return nil
			}

			cli := kvproto.NewKVClient(cc)
			_, err = cli.Set(
				node.WithClientKey(context.Background(), keyGen.Get(args[0])),
				&kvproto.SetRequest{Key: args[0], Value: args[1]},
			)
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to get key: %s", err)
				return nil
			}
			return nil
		},
	}

	cmd.AddCommand(getCmd, setCmd)
	_ = cmd.Execute()
}
