package registry

import (
	"context"
	"go.etcd.io/etcd/client"
	"testing"
	"time"
)

var session client.Client
var keyApi client.KeysAPI
var topics = make([]string, 0, 10)

func testInit() {

	keyApi.Delete(context.Background(), "/kiteq", &client.DeleteOptions{Recursive: true})
	time.Sleep(1 * time.Second)
}

func init() {
	cfg := client.Config{
		Endpoints: []string{"http://localhost:2379"},
		Transport: client.DefaultTransport,
		// set timeout per request to fail fast when the target endpoint is unavailable
		HeaderTimeoutPerRequest: 5 * time.Second}
	session, _ = client.New(cfg)
	keyApi = client.NewKeysAPI(session)

	topics = append(topics, "trade")
	topics = append(topics, "message")
}

func TestQServerWroker_Watcher(t *testing.T) {

	testInit()

	//watcher
	watcher := &QServersWatcher{Api: keyApi, Topics: topics, Watcher: &MockWatcher{}, cancelWatch: false}
	watcher.Watch()

	// Hostport        string
	// Topics          []string
	// KeepalivePeriod time.Duration
	worker := &QServerWroker{Api: keyApi, Topics: topics,
		KeepalivePeriod: 2 * time.Second, Hostport: "localhost:8080"}
	worker.Start()
	time.Sleep(100 * time.Millisecond)

	qpath := KITEQ_SERVER + "/" + "trade"

	running := true
	go func() {
		for running {
			worker.Keepalive()
			time.Sleep(2 * time.Second)
		}
	}()

	time.Sleep(10 * time.Second)
	resp, err := keyApi.Get(context.Background(), qpath, &client.GetOptions{Recursive: false})
	if nil != err {
		t.Fail()
		t.Log(err)
		return
	}

	if resp.Node.Nodes.Len() <= 0 {
		t.Fail()
		t.Logf("TestQServerWroker|Schedule Refresh|%s\n", resp.Node.Nodes)
	}

	for _, n := range resp.Node.Nodes {
		t.Logf("%s,kiteqServer:%s", qpath, n.Key)
	}

	resp, err = keyApi.Get(context.Background(), qpath, &client.GetOptions{Sort: true, Recursive: true})
	if nil != err {
		t.Fail()
		t.Log(err)
		return
	}

	for _, n := range resp.Node.Nodes {
		t.Logf(n.Key)
	}
	running = false
	// //wait expired
	time.Sleep(10 * time.Second)

	resp, err = keyApi.Get(context.Background(), qpath, &client.GetOptions{Sort: true, Recursive: true})
	if nil != err {
		t.Fail()
		t.Log(err)
		return
	}

	if resp.Node.Nodes.Len() > 0 {
		t.Fail()
		t.Logf("Not Expired Node %s\t%v\n", qpath, resp.Node.Nodes)
	}

}

//
func TestQClientWorker_Watcher(t *testing.T) {

	testInit()

	binds := make([]*Binding, 0, 2)
	binds = append(binds, Bind_Direct("s-mts-group", "message", "p2p", -1, true))
	binds = append(binds, Bind_Direct("s-mts-group", "trade", "pay-succ", -1, true))

	// 	Api     client.KeysAPI
	// Topics  []string
	// Watcher IWatcher
	watcher := &BindWatcher{Api: keyApi, Topics: topics, Watcher: &MockWatcher{}, cancelWatch: false}
	watcher.Watch()

	// Api             client.KeysAPI
	// PublishTopics   []string
	// Hostport        string
	// GroupId         string
	// Bindings        []*Binding
	// KeepalivePeriod time.Duration
	worker := &QClientWorker{Api: keyApi, PublishTopics: topics,
		Hostport: "localhost:13001", GroupId: "s-mts-group",
		Bindings: binds, KeepalivePeriod: 5 * time.Second}
	worker.Start()
	running := true
	go func() {
		for running {
			worker.Keepalive()
			time.Sleep(2 * time.Second)
		}
	}()

	time.Sleep(10 * time.Second)

	gb, ok := watcher.binds["trade"]
	if !ok {
		t.Fail()
		t.Log("--------watch trade --------")
		return
	}

	t.Logf("bind:%v", gb)

	subbinds := gb["s-mts-group"]

	if len(subbinds) != 1 {
		t.Fail()
		t.Log(subbinds)
		return
	}

	if subbinds[0].Topic != "trade" {
		t.Fail()
		t.Log(subbinds[0])
	}

}
