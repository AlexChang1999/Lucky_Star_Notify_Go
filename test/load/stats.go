package main

import (
	"fmt"
	"slices"
	"time"
)

// summary 是一組延遲樣本的摘要。
//
// 只留 min / p50 / p95 / p99 / max，不放平均值——**平均值對延遲是個壞指標**：
// 長尾一拉高它就跟著跑，但它既不代表典型使用者的體感（那是 p50），
// 也不代表最糟情況（那是 p99/max）。報告裡放平均值只會招來
// 「那 p99 呢」這一問。
type summary struct {
	Count int   `json:"count"`
	MinNs int64 `json:"minNs"`
	P50Ns int64 `json:"p50Ns"`
	P95Ns int64 `json:"p95Ns"`
	P99Ns int64 `json:"p99Ns"`
	MaxNs int64 `json:"maxNs"`
}

// summarize 排序樣本並算出百分位數。
//
// 會**就地排序**傳進來的 slice。呼叫端都是「算完就不再用原順序」，
// 為此多配置一份副本沒有意義；但這件事得寫出來，不然下一個人會踩到。
func summarize(samples []time.Duration) summary {
	if len(samples) == 0 {
		return summary{}
	}
	slices.Sort(samples)
	return summary{
		Count: len(samples),
		MinNs: samples[0].Nanoseconds(),
		P50Ns: percentile(samples, 50).Nanoseconds(),
		P95Ns: percentile(samples, 95).Nanoseconds(),
		P99Ns: percentile(samples, 99).Nanoseconds(),
		MaxNs: samples[len(samples)-1].Nanoseconds(),
	}
}

// percentile 用**最近排名法**（nearest-rank）取百分位數。
//
// 也就是「第 ceil(p/100 × N) 個樣本」，不做插值。理由是插值會產生
// 一個資料裡不存在的數字——對延遲報告來說，「有一個請求真的花了這麼久」
// 比「理論上的分位點」誠實。Prometheus 的 histogram_quantile 會插值，
// 所以兩邊的數字本來就不該期待完全一致。
func percentile(sorted []time.Duration, p int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	rank := (p*len(sorted) + 99) / 100 // ceil(p/100 * N)，整數運算
	if rank < 1 {
		rank = 1
	}
	if rank > len(sorted) {
		rank = len(sorted)
	}
	return sorted[rank-1]
}

// String 讓摘要能直接印進報告。
func (s summary) String() string {
	if s.Count == 0 {
		return "（沒有樣本）"
	}
	return fmt.Sprintf("n=%d  min %s  p50 %s  p95 %s  p99 %s  max %s",
		s.Count,
		round(s.MinNs), round(s.P50Ns), round(s.P95Ns), round(s.P99Ns), round(s.MaxNs))
}

// round 把奈秒印成人看得懂的長度，並砍掉沒有意義的位數。
//
// 毫秒級的數字留到小數點後兩位（0.01ms = 10µs，已經遠低於量測誤差），
// 微秒級的就直接印微秒。
func round(ns int64) string {
	d := time.Duration(ns)
	if d >= time.Millisecond {
		return d.Round(10 * time.Microsecond).String()
	}
	return d.Round(time.Microsecond).String()
}
