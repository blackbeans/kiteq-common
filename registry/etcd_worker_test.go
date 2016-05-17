package registry

import (
	"github.com/coreos/etcd/client"
	"golang.org/x/net/context"
	"testing"
	"time"
)

var session client.Client
var keyApi client.KeysAPI

func init() {
	cfg := client.Config{
		Endpoints: []string{"http://localhost:2379"},
		Transport: client.DefaultTransport,
		// set timeout per request to fail fast when the target endpoint is unavailable
		HeaderTimeoutPerRequest: 5 * time.Second}
	session, _ = client.New(cfg)
	keyApi = client.NewKeysAPI(session)
}

func TestQServerWroker(t *testing.T) {

	topics := make([]string, 0, 10)
	topics = append(topics, "trade")
	topics = append(topics, "message")
	for _, t := range topics {
		qpath := KITEQ_SERVER + "/" + t
		keyApi.Delete(context.Background(), qpath, &client.DeleteOptions{Recursive: true})
	}
	time.Sleep(10 * time.Second)

	// Hostport        string
	// Topics          []string
	// KeepalivePeriod time.Duration

	worker := &QServerWroker{Api: keyApi, Topics: topics, KeepalivePeriod: 2 * time.Second, Hostport: "localhost:8080"}

	worker.Keepalive()

	time.Sleep(100 * time.Millisecond)

	qpath := KITEQ_SERVER + "/" + "trade"

	resp, err := keyApi.Get(context.Background(), qpath, &client.GetOptions{Sort: true, Recursive: true})
	if nil != err {
		t.Fail()
		t.Log(err)
		return
	}

	for _, n := range resp.Node.Nodes {
		t.Logf(n.Key)
	}

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

	go func() {
		for {
			worker.Keepalive()
			time.Sleep(2 * time.Second)
		}
	}()

	time.Sleep(10 * time.Second)
	resp, err = keyApi.Get(context.Background(), qpath, &client.GetOptions{Recursive: true})
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

}
