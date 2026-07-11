package pregel

import (
	"context"
	"fmt"
)

// Node is a function that takes a pointer to the state, mutates it,
// and returns the string name of the next node to execute.
type Node[T any] func(ctx context.Context, state *T) (string, error)

type Graph[T any] struct {
	nodes map[string]Node[T]
	start string
}

func NewGraph[T any]() *Graph[T] {
	return &Graph[T]{
		nodes: make(map[string]Node[T]),
	}
}

func (g *Graph[T]) AddNode(name string, n Node[T]) {
	g.nodes[name] = n
}

func (g *Graph[T]) SetEntryPoint(name string) {
	g.start = name
}

// Run executes the graph loop. It passes the state pointer from node to node
// until a node returns "END" or an empty string.
func (g *Graph[T]) Run(ctx context.Context, state *T) error {
	curr := g.start
	for curr != "" && curr != "END" {
		nodeFunc, exists := g.nodes[curr]
		if !exists {
			return fmt.Errorf("node %s not found in graph layout", curr)
		}

		next, err := nodeFunc(ctx, state)
		if err != nil {
			return fmt.Errorf("execution crashed at node %s: %w", curr, err)
		}
		curr = next
	}
	return nil
}
