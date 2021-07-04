// Package nodepb holds protobuf descriptions for a node within a
// Croissant cluster.
package nodepb

//go:generate protoc -I=../../api --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative ../../api/node.proto
