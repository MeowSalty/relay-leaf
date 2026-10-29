package observe

import (
	"fmt"
	"sync"
	"testing"

	"relayleaf/internal/protocol"
)

func TestStoreBoundsHistoryAndCopiesUsage(t *testing.T) {
	store := NewStore(2)
	input := int64(17)
	record := RequestRecord{ID: "request-one", Attempts: 1, Attempt: &AttemptRecord{ID: "attempt-one", Response: protocol.ResponseFacts{Usage: protocol.Usage{Status: "partial", Input: &input}}}}
	store.Append(record)
	input = 99
	record.Attempt.ID = "changed"
	first := store.Recent()
	if len(first) != 1 || first[0].Attempt.ID != "attempt-one" || *first[0].Attempt.Response.Usage.Input != 17 || first[0].Attempt.Response.Usage.Output != nil {
		t.Fatalf("入队后事实被修改或未知用量被补零：%+v", first)
	}
	*first[0].Attempt.Response.Usage.Input = 88
	first[0].Attempt.ID = "changed-again"
	if saved := store.Recent()[0]; saved.Attempt.ID != "attempt-one" || *saved.Attempt.Response.Usage.Input != 17 {
		t.Fatal("读取历史时暴露了可修改的内部事实")
	}
	store.Append(RequestRecord{ID: "request-two"})
	store.Append(RequestRecord{ID: "request-three"})
	recent := store.Recent()
	if len(recent) != 2 || recent[0].ID != "request-three" || recent[1].ID != "request-two" || store.Evicted() != 1 {
		t.Fatalf("窗口没有按完成顺序保留最近记录：%+v，淘汰=%d", recent, store.Evicted())
	}
}

func TestStoreConcurrentRequestsKeepAssociations(t *testing.T) {
	const capacity, requests = 8, 64
	store := NewStore(capacity)
	var work sync.WaitGroup
	for i := range requests {
		work.Go(func() {
			id := fmt.Sprint(i)
			usage := int64(i)
			store.Append(RequestRecord{ID: id, Attempts: 1, Attempt: &AttemptRecord{ID: id, Response: protocol.ResponseFacts{Usage: protocol.Usage{Input: &usage}}}})
			for _, record := range store.Recent() {
				if record.Attempt == nil || record.ID != record.Attempt.ID || record.ID != fmt.Sprint(*record.Attempt.Response.Usage.Input) {
					t.Errorf("并发请求的尝试与用量关联不一致：%+v", record)
				}
			}
		})
	}
	work.Wait()
	if len(store.Recent()) != capacity || store.Evicted() != requests-capacity {
		t.Fatalf("并发写入突破历史窗口：保留=%d，淘汰=%d", len(store.Recent()), store.Evicted())
	}
}
