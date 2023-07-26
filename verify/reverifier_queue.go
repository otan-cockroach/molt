package verify

import "container/heap"

// reverifyQueue implements the heap interface.
type reverifyQueue []*RetryItem

var _ heap.Interface = (*reverifyQueue)(nil)

func (rq *reverifyQueue) heapPush(it *RetryItem) {
	heap.Push(rq, it)
}

func (rq *reverifyQueue) heapPop() *RetryItem {
	return heap.Pop(rq).(*RetryItem)
}

func (rq reverifyQueue) Len() int { return len(rq) }

func (rq reverifyQueue) Less(i, j int) bool {
	return rq[i].Retry.NextRetry.Before(rq[j].Retry.NextRetry)
}

func (rq reverifyQueue) Swap(i, j int) {
	rq[i], rq[j] = rq[j], rq[i]
}

func (rq *reverifyQueue) Push(x any) {
	*rq = append(*rq, x.(*RetryItem))
}

func (rq *reverifyQueue) Pop() any {
	old := *rq
	n := len(old)
	item := old[n-1]
	*rq = old[0 : n-1]
	return item
}
