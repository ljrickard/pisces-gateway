package pregel

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

func (n *GatewayNodes) ExecuteWorkers(ctx context.Context, state *AgentState) (string, error) {
	if state.StatusStream != nil {
		state.StatusStream("Executing tasks in parallel...")
	}
	slog.Info("[Node] Executing ExecuteWorkers (Fan-Out)", "tasks", len(state.Tasks))
	state.LoopCount++

	var wg sync.WaitGroup
	var mu sync.Mutex

	for i := range state.Tasks {
		wg.Add(1)

		go func(index int) {
			defer wg.Done()
			task := state.Tasks[index]

			slog.Info("▶️ [Worker Started]", "domain", task.Domain, "query", task.Query)

			worker, exists := n.Workers[task.Domain]
			if !exists {
				slog.Warn("Unknown domain, falling back to generic worker", "domain", task.Domain)
				worker = n.Workers["generic"]
			}

			answer, err := worker(ctx, task.Query, state.ReqConfig, state.SessionID)

			if err != nil {
				slog.Error("❌ [Worker Failed]", "domain", task.Domain, "error", err)
				answer = "I'm sorry, I couldn't reach the " + task.Domain + " services right now."
			} else {
				slog.Info("✅ [Worker Completed]", "domain", task.Domain, "answer_length", len(answer))
			}

			mu.Lock()
			state.Tasks[index].Answer = answer
			mu.Unlock()

		}(i)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	ticker := time.NewTicker(4 * time.Second)
	defer ticker.Stop()

	secondsWaiting := 0

	for {
		select {
		case <-done:
			slog.Info("Parallel execution complete. Fanning in...")
			return "synthesizer_node", nil

		case <-ticker.C:
			// Increment by 4 since our ticker is 4 seconds
			secondsWaiting += 4

			// 💓 Pump a dynamic heartbeat to the Stream!
			if state.StatusStream != nil {
				msg := fmt.Sprintf("Scanning databases across domains (%ds)...", secondsWaiting)
				state.StatusStream(msg)
			}
			slog.Debug("Waiting on workers, heartbeat pumped...", "seconds", secondsWaiting)
		}
	}
}
