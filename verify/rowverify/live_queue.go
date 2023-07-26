package rowverify

import "container/heap"

// liveQueue implements the heap interface.
type liveQueue struct {
	items  []*liveItem
	numPKs int
}

var _ heap.Interface = (*liveQueue)(nil)

func (rq *liveQueue) heapPush(it *liveItem) {
	heap.Push(rq, it)
}

func (rq *liveQueue) heapPop() *liveItem {
	return heap.Pop(rq).(*liveItem)
}

func (rq liveQueue) Len() int { return len(rq.items) }

func (rq liveQueue) Less(i, j int) bool {
	return rq.items[i].Retry.NextRetry.Before(rq.items[j].Retry.NextRetry)
}

func (rq liveQueue) Swap(i, j int) {
	rq.items[i], rq.items[j] = rq.items[j], rq.items[i]
}

func (rq *liveQueue) Push(x any) {
	rq.items = append(rq.items, x.(*liveItem))
	rq.numPKs += len(x.(*liveItem).PrimaryKeys)
}

func (rq *liveQueue) Pop() any {
	old := rq.items
	n := len(old)
	item := old[n-1]
	rq.items = old[0 : n-1]
	rq.numPKs -= len(item.PrimaryKeys)
	return item
}
