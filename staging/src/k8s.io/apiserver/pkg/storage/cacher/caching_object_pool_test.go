/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cacher

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/apitesting"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/apis/example"
	examplev1 "k8s.io/apiserver/pkg/apis/example/v1"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/cacher/metrics"
	"k8s.io/client-go/tools/cache"
	compbasemetrics "k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/testutil"
)

func passEverything(_ string, _ labels.Set, _ fields.Set) bool { return true }

func makePooledPod(name string, resourceVersion uint64) *v1.Pod {
	pod := makeTestPod(name, resourceVersion)
	pod.UID = types.UID("uid-" + name)
	return pod
}

func newPooledWatcher(pool *cachingObjectPool, filter filterWithAttrsFunc) *cacheWatcher {
	return &cacheWatcher{filter: filter, versioner: storage.APIObjectVersioner{}, objectPool: pool}
}

func makeReplayedEvent(pod interface {
	apiruntime.Object
	GetLabels() map[string]string
}, rv uint64) *watchCacheEvent {
	return &watchCacheEvent{
		Type:            watch.Added,
		Object:          pod,
		ObjLabels:       labels.Set(pod.GetLabels()),
		ObjFields:       fields.Set{},
		Key:             "/prefix/ns/foo",
		ResourceVersion: rv,
	}
}

// TestCachingObjectPoolSharesWrapperAcrossWatchers checks replaying watchers share one wrapper.
func TestCachingObjectPoolSharesWrapperAcrossWatchers(t *testing.T) {
	pool := newCachingObjectPool(schema.GroupResource{Resource: "pods"})
	pod := makePooledPod("foo", 10)
	event := makeReplayedEvent(pod, 10)

	e1 := newPooledWatcher(pool, passEverything).convertToWatchEvent(event)
	e2 := newPooledWatcher(pool, passEverything).convertToWatchEvent(event)

	co, ok := e1.Object.(*cachingObject)
	if !ok {
		t.Fatalf("expected *cachingObject, got %T", e1.Object)
	}
	if e1.Object != e2.Object {
		t.Errorf("expected both watchers to share one cachingObject, got distinct objects")
	}
	if co.object != apiruntime.Object(pod) {
		t.Errorf("wrapper does not wrap the original object")
	}

	// A distinct object with the same uid and resourceVersion resolves to the same wrapper.
	copied := makeReplayedEvent(pod.DeepCopy(), 10)
	if e3 := newPooledWatcher(pool, passEverything).convertToWatchEvent(copied); e3.Object != e1.Object {
		t.Errorf("expected the copied object to share the wrapper")
	}
}

// TestCachingObjectPoolRejectsObjectsWithoutIdentity checks objects lacking uid or rv are not shared.
func TestCachingObjectPoolRejectsObjectsWithoutIdentity(t *testing.T) {
	pool := newCachingObjectPool(schema.GroupResource{Resource: "pods"})
	noUID := makeTestPod("foo", 10)
	noRV := makeTestPod("bar", 0)
	noRV.UID, noRV.ResourceVersion = "uid-bar", ""
	for _, pod := range []*v1.Pod{noUID, noRV} {
		if _, err := pool.newCachingObject(pod); err == nil {
			t.Errorf("expected an error for %q", pod.Name)
		}
	}
	e := newPooledWatcher(pool, passEverything).convertToWatchEvent(makeReplayedEvent(noUID, 10))
	if _, ok := e.Object.(*cachingObject); ok {
		t.Errorf("expected a plain deep copy for an object without uid, got *cachingObject")
	}
	if n := poolLen(pool); n != 0 {
		t.Errorf("expected the pool to stay empty, got %d entries", n)
	}
}

// TestCachingObjectPoolNilFallsBackToDeepCopy checks a nil pool keeps per-watcher deep copies.
func TestCachingObjectPoolNilFallsBackToDeepCopy(t *testing.T) {
	pod := makeTestPod("foo", 10)
	event := makeReplayedEvent(pod, 10)

	w := newPooledWatcher(nil, passEverything)
	e1 := w.convertToWatchEvent(event)
	e2 := w.convertToWatchEvent(event)
	if e1.Object == e2.Object {
		t.Errorf("expected independent deep copies without a pool")
	}
	if _, ok := e1.Object.(*cachingObject); ok {
		t.Errorf("expected a plain deep copy without a pool, got *cachingObject")
	}
}

// TestCachingObjectPoolSharesLiveWrapperWithReplay checks a live-dispatched wrapper is reused by later replays.
func TestCachingObjectPoolSharesLiveWrapperWithReplay(t *testing.T) {
	pool := newCachingObjectPool(schema.GroupResource{Resource: "pods"})
	pod := makePooledPod("foo", 10)
	live := makeReplayedEvent(pod, 10)
	setCachingObjects(pool, live, storage.APIObjectVersioner{})

	co, ok := live.Object.(*cachingObject)
	if !ok {
		t.Fatalf("expected the live event to carry a *cachingObject, got %T", live.Object)
	}
	e := newPooledWatcher(pool, passEverything).convertToWatchEvent(live)
	if e.Object != live.Object {
		t.Errorf("expected the live cachingObject to be passed through")
	}
	// A replay of the same object version, from the raw ring-buffer event, hits the same wrapper.
	replayed := makeReplayedEvent(pod, 10)
	if r := newPooledWatcher(pool, passEverything).convertToWatchEvent(replayed); r.Object != apiruntime.Object(co) {
		t.Errorf("expected the replay to reuse the live wrapper")
	}
}

// TestCachingObjectPoolLiveDeletedNotPooled checks the DELETE-style live wrapper stays private.
func TestCachingObjectPoolLiveDeletedNotPooled(t *testing.T) {
	pool := newCachingObjectPool(schema.GroupResource{Resource: "pods"})
	prev := makePooledPod("foo", 5)
	event := &watchCacheEvent{Type: watch.Deleted, PrevObject: prev, ResourceVersion: 6}
	setCachingObjects(pool, event, storage.APIObjectVersioner{})
	if _, ok := event.PrevObject.(*cachingObject); !ok {
		t.Fatalf("expected PrevObject to be wrapped, got %T", event.PrevObject)
	}
	if n := poolLen(pool); n != 0 {
		t.Errorf("expected the deleted wrapper to stay out of the pool, got %d entries", n)
	}
	if rv := event.PrevObject.(*cachingObject).GetResourceVersion(); rv != "6" {
		t.Errorf("expected the event's resourceVersion 6, got %q", rv)
	}
}

// TestCachingObjectPoolLiveWithoutIdentityStillWrapped checks uid-less live objects keep a private wrapper.
func TestCachingObjectPoolLiveWithoutIdentityStillWrapped(t *testing.T) {
	pool := newCachingObjectPool(schema.GroupResource{Resource: "pods"})
	event := makeReplayedEvent(makeTestPod("nouid", 10), 10)
	setCachingObjects(pool, event, storage.APIObjectVersioner{})
	if _, ok := event.Object.(*cachingObject); !ok {
		t.Fatalf("expected a private cachingObject, got %T", event.Object)
	}
	if n := poolLen(pool); n != 0 {
		t.Errorf("expected the pool to stay empty, got %d entries", n)
	}
}

// TestCachingObjectPoolStoreIntervalShares checks store-synthesized events share one wrapper.
func TestCachingObjectPoolStoreIntervalShares(t *testing.T) {
	pool := newCachingObjectPool(schema.GroupResource{Resource: "pods"})
	store := newStoreIndexer(&cache.Indexers{})
	if err := store.Add(makeTestStoreElement(makePooledPod("pod1", 1))); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	next := func() *watchCacheEvent {
		ci, err := newCacheIntervalFromStore(1, store, "", false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		ev, err := ci.Next()
		if err != nil || ev == nil {
			t.Fatalf("unexpected Next result: %v %v", ev, err)
		}
		return ev
	}

	o1 := newPooledWatcher(pool, passEverything).convertToWatchEvent(next()).Object
	o2 := newPooledWatcher(pool, passEverything).convertToWatchEvent(next()).Object
	if o1 != o2 {
		t.Errorf("expected watchers replaying from store to share one cachingObject")
	}
}

func makeFilterTransitionEvent() (*watchCacheEvent, interface{ GetResourceVersion() string }) {
	prevPod := makeTestPod("foo", 5)
	prevPod.Labels = map[string]string{"keep": "true"}
	curPod := makeTestPod("foo", 6)
	return &watchCacheEvent{
		Type:            watch.Modified,
		Object:          curPod,
		ObjLabels:       labels.Set(curPod.Labels),
		ObjFields:       fields.Set{"spec.nodeName": curPod.Spec.NodeName},
		PrevObject:      prevPod,
		PrevObjLabels:   labels.Set(prevPod.Labels),
		PrevObjFields:   fields.Set{"spec.nodeName": prevPod.Spec.NodeName},
		Key:             "/prefix/ns/foo",
		ResourceVersion: 6,
	}, prevPod
}

func keepOnly(_ string, l labels.Set, _ fields.Set) bool { return l["keep"] == "true" }

// TestCachingObjectPoolDeletedReplay checks DELETE-style delivery bypasses the pool.
func TestCachingObjectPoolDeletedReplay(t *testing.T) {
	pool := newCachingObjectPool(schema.GroupResource{Resource: "pods"})
	event, prevPod := makeFilterTransitionEvent()

	e1 := newPooledWatcher(pool, keepOnly).convertToWatchEvent(event)
	e2 := newPooledWatcher(pool, keepOnly).convertToWatchEvent(event)
	if e1.Type != watch.Deleted || e2.Type != watch.Deleted {
		t.Fatalf("expected Deleted delivery, got %v/%v", e1.Type, e2.Type)
	}
	if e1.Object == e2.Object {
		t.Errorf("expected independent copies for deleted delivery")
	}
	if _, ok := e1.Object.(*cachingObject); ok {
		t.Fatalf("expected a plain deep copy for deleted delivery, got *cachingObject")
	}
	if rv := e1.Object.(interface{ GetResourceVersion() string }).GetResourceVersion(); rv != "6" {
		t.Errorf("expected resourceVersion 6, got %q", rv)
	}
	if prevPod.GetResourceVersion() != "5" {
		t.Errorf("original PrevObject mutated: rv=%q", prevPod.GetResourceVersion())
	}
	if n := poolLen(pool); n != 0 {
		t.Errorf("expected the pool to stay empty, got %d entries", n)
	}
}

// TestCachingObjectPoolConcurrentNoRace races convert+encode on one shared wrapper.
func TestCachingObjectPoolConcurrentNoRace(t *testing.T) {
	pool := newCachingObjectPool(schema.GroupResource{Resource: "pods"})
	event := makeReplayedEvent(makePooledPod("foo", 10), 10)

	const workers = 32
	results := make([]*watch.Event, workers)
	encoded := make([][]byte, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			e := newPooledWatcher(pool, passEverything).convertToWatchEvent(event)
			results[i] = e
			co := e.Object.(*cachingObject)
			buf := bytes.NewBuffer(nil)
			err := co.CacheEncode("test-id", func(obj apiruntime.Object, w io.Writer) error {
				_, err := fmt.Fprintf(w, "rv=%s", obj.(interface{ GetResourceVersion() string }).GetResourceVersion())
				return err
			}, buf)
			if err != nil {
				t.Errorf("unexpected encode error: %v", err)
			}
			encoded[i] = buf.Bytes()
		}(i)
	}
	wg.Wait()

	for i := 1; i < workers; i++ {
		if results[i].Object != results[0].Object {
			t.Fatalf("worker %d got a different wrapper", i)
		}
	}
	for i, b := range encoded {
		if string(b) != "rv=10" {
			t.Errorf("worker %d observed stale encode: %q", i, string(b))
		}
	}
}

// poolLen counts the wrappers held across both generations.
func poolLen(pool *cachingObjectPool) int {
	n := 0
	generations := pool.generations.Load()
	for _, m := range []*sync.Map{generations.current, generations.previous} {
		m.Range(func(_, _ any) bool { n++; return true })
	}
	return n
}

// TestCachingObjectPoolRotation checks a wrapper survives one rotation and is dropped by the second.
func TestCachingObjectPoolRotation(t *testing.T) {
	pool := newCachingObjectPool(schema.GroupResource{Resource: "pods"})
	podA, podB := makePooledPod("a", 1), makePooledPod("b", 2)

	a1, err := pool.newCachingObject(podA)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	pool.rotate()
	// podA now lives in the previous generation and is still shared.
	a2, err := pool.newCachingObject(podA)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if a1 != a2 {
		t.Errorf("expected the wrapper to be reused from the previous generation")
	}
	// podB is stored in the current generation.
	b1, err := pool.newCachingObject(podB)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n := poolLen(pool); n != 2 {
		t.Errorf("expected 2 wrappers across generations, got %d", n)
	}

	pool.rotate()
	// podA's generation was dropped; podB moved to previous and is still shared.
	if n := poolLen(pool); n != 1 {
		t.Errorf("expected only podB to remain, got %d wrappers", n)
	}
	a3, err := pool.newCachingObject(podA)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if a1 == a3 {
		t.Errorf("expected a new wrapper for podA after two rotations")
	}
	b2, err := pool.newCachingObject(podB)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if b1 != b2 {
		t.Errorf("expected podB's wrapper to be reused from the previous generation")
	}
}

// TestCachingObjectPoolPurge checks purge drops every wrapper.
func TestCachingObjectPoolPurge(t *testing.T) {
	pool := newCachingObjectPool(schema.GroupResource{Resource: "pods"})
	podA, podB := makePooledPod("a", 1), makePooledPod("b", 2)
	a1, err := pool.newCachingObject(podA)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := pool.newCachingObject(podB); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	pool.rotate()
	if _, err := pool.newCachingObject(podB); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	pool.purge()
	if n := poolLen(pool); n != 0 {
		t.Fatalf("expected an empty pool after purge, got %d wrappers", n)
	}
	a2, err := pool.newCachingObject(podA)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if a1 == a2 {
		t.Errorf("expected a new wrapper after purge")
	}
}

// TestCachingObjectPoolLookupMetrics checks hits and misses are counted per resource.
func TestCachingObjectPoolLookupMetrics(t *testing.T) {
	registerPoolLookupMetric(t)
	gr := schema.GroupResource{Group: "test.io", Resource: "widgets"}
	read := func(result string) float64 {
		v, err := testutil.GetCounterMetricValue(metrics.CachingObjectPoolLookups.WithLabelValues(gr.Group, gr.Resource, result))
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	hits0, misses0 := read("hit"), read("miss")

	pool := newCachingObjectPool(gr)
	podA, podB := makePooledPod("a", 1), makePooledPod("b", 2)
	for _, pod := range []*v1.Pod{podA, podA, podB, podA} {
		if _, err := pool.newCachingObject(pod); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.newCachingObject(makeTestPod("nouid", 3)); err == nil {
		t.Fatal("expected an error for an object without uid")
	}
	if got := read("miss") - misses0; got != 2 {
		t.Errorf("misses = %v, want 2", got)
	}
	if got := read("hit") - hits0; got != 2 {
		t.Errorf("hits = %v, want 2", got)
	}
}

// registerPoolLookupMetric makes the pool count lookups the way a registered apiserver does.
func registerPoolLookupMetric(tb testing.TB) {
	registry := compbasemetrics.NewKubeRegistry()
	registry.MustRegister(metrics.CachingObjectPoolLookups)
	tb.Cleanup(registry.Reset)
}

func makeBenchmarkPods(n int) []*example.Pod {
	annotations := make(map[string]string, 20)
	for j := 0; j < 20; j++ {
		annotations[fmt.Sprintf("example.com/annotation-%d", j)] = strings.Repeat("x", 100)
	}
	pods := make([]*example.Pod, n)
	for i := range pods {
		pods[i] = &example.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:       "ns",
				Name:            fmt.Sprintf("pod-%d", i),
				UID:             types.UID(fmt.Sprintf("uid-%d", i)),
				ResourceVersion: strconv.Itoa(i + 1),
				Labels:          map[string]string{"app": "bench", "tier": "backend"},
				Annotations:     annotations,
			},
			Spec: example.PodSpec{NodeName: "node-1", ServiceAccountName: "default"},
		}
	}
	return pods
}

// BenchmarkReplayConvertAndEncode measures replaying the same events to many watchers, with and without the pool.
func BenchmarkReplayConvertAndEncode(b *testing.B) {
	registerPoolLookupMetric(b)
	const watchers, objects = 32, 100
	codec := apitesting.TestCodec(codecs, examplev1.SchemeGroupVersion)
	encode := func(obj apiruntime.Object, w io.Writer) error { return codec.Encode(obj, w) }
	events := make([]*watchCacheEvent, objects)
	for i, pod := range makeBenchmarkPods(objects) {
		events[i] = makeReplayedEvent(pod, uint64(i+1))
	}

	for _, tc := range []struct {
		name string
		pool func() *cachingObjectPool
	}{
		{name: "DeepCopy", pool: func() *cachingObjectPool { return nil }},
		{name: "Pool", pool: func() *cachingObjectPool { return newCachingObjectPool(schema.GroupResource{Resource: "pods"}) }},
	} {
		b.Run(tc.name, func(b *testing.B) {
			pool := tc.pool()
			ws := make([]*cacheWatcher, watchers)
			for i := range ws {
				ws[i] = newPooledWatcher(pool, passEverything)
			}
			buf := bytes.NewBuffer(nil)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				e := ws[i%watchers].convertToWatchEvent(events[(i/watchers)%objects])
				buf.Reset()
				var err error
				if co, ok := e.Object.(apiruntime.CacheableObject); ok {
					err = co.CacheEncode("bench", encode, buf)
				} else {
					err = encode(e.Object, buf)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkCachingObjectPoolGet measures the pool lookup path under concurrent access.
func BenchmarkCachingObjectPoolGet(b *testing.B) {
	registerPoolLookupMetric(b)
	for _, objects := range []int{1, 100, 10000} {
		b.Run(fmt.Sprintf("objects=%d", objects), func(b *testing.B) {
			pool := newCachingObjectPool(schema.GroupResource{Resource: "pods"})
			pods := makeBenchmarkPods(objects)
			for _, pod := range pods {
				if _, err := pool.newCachingObject(pod); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					if _, err := pool.newCachingObject(pods[i%objects]); err != nil {
						b.Fatal(err)
					}
					i++
				}
			})
		})
	}
}
