package registry

import (
	"github.com/blackbeans/kiteq-common/registry/bind"
	"github.com/coreos/etcd/client"
	"golang.org/x/net/context"
	"testing"
	"time"
)

var etcdRegistry *EtcdRegistry

func init() {
	etcdRegistry = NewEtcdRegistry("http://localhost:2379")
	etcdRegistry.Start()
}

// func registryInit() {
// 	etcdRegistry.api.Delete(context.Background(), "/kiteq", &client.DeleteOptions{Recursive: true})
// 	etcdRegistry.PublishQServer("localhost:13000", []string{"trade"})
// 	time.Sleep(5 * time.Second)
// }

//publish/unpublish qserver and watch test
func TestEtcdPublish_UnPublish_QServer(t *testing.T) {

	etcdRegistry.api.Delete(context.Background(), "/kiteq", &client.DeleteOptions{Recursive: true})
	etcdRegistry.PublishQServer("localhost:13000", []string{"trade"})
	time.Sleep(1 * time.Second)

	servers, err := etcdRegistry.GetQServerAndWatch("trade")
	if nil != err {
		t.Error(err)
		t.Fail()
		return
	}
	if len(servers) > 1 || len(servers) <= 0 {
		t.Fail()
		t.Log("TestEtcdPublishQServer|GetQServerAndWatch|FAIL|%s", servers)
		return
	}

	t.Logf("TestEtcdPublishQServer|GetQServerAndWatch|SUCC|%v\n", servers)
	t.Log("------------Add New KiteQServer : localhost:13001 Test Watch---------")
	etcdRegistry.PublishQServer("localhost:13001", []string{"trade"})
	time.Sleep(1 * time.Second)

	servers, err = etcdRegistry.GetQServerAndWatch("trade")
	if nil != err {
		t.Error(err)
		t.Fail()
		return
	}
	if len(servers) < 2 {
		t.Fail()
		t.Logf("TestEtcdPublishQServer|GetQServerAndWatch|FAIL|%s\n", servers)
		return
	}

	t.Logf("TestEtcdPublishQServer|GetQServerAndWatch|SUCC|%v\n", servers)
	time.Sleep(1 * time.Second)
	t.Log("------------DELET  KiteQServer : localhost:1300 Test Watch---------")
	etcdRegistry.UnpushlishQServer("localhost:13001", []string{"trade"})
	servers, err = etcdRegistry.GetQServerAndWatch("trade")
	if nil != err {
		t.Error(err)
		t.Fail()
		return
	}
	if len(servers) > 1 || len(servers) <= 0 {
		t.Fail()
		t.Log("TestEtcdPublishQServer|GetQServerAndWatch|FAIL|%s", servers)
		return
	}
	t.Logf("TestEtcdPublishQServer|GetQServerAndWatch|SUCC|%v\n", servers)
	time.Sleep(5 * time.Second)
}

//subscribe bind  and watch test
func TestEtcdSubscribe_QServer(t *testing.T) {

	etcdRegistry.api.Delete(context.Background(), "/kiteq", &client.DeleteOptions{Recursive: true})

	binds := make([]*bind.Binding, 0, 2)
	binds = append(binds, bind.Bind_Direct("s-mts-group", "message", "p2p", -1, true))
	binds = append(binds, bind.Bind_Direct("s-mts-group", "trade", "pay-succ", -1, true))
	etcdRegistry.PublishBindings("s-mts-group", binds)

	time.Sleep(5 * time.Second)

	bindings, err := etcdRegistry.GetBindAndWatch("trade")
	if nil != err {
		t.Error(err)
		t.Fail()
		return
	}
	if len(bindings) > 1 || len(bindings) <= 0 {
		t.Fail()
		t.Log("TestEtcdSubscribe_QServer|GetBindAndWatch|FAIL|%s", bindings)
		return
	}

	t.Logf("TestEtcdSubscribe_QServer|GetBindAndWatch|SUCC|%v\n", bindings)
	t.Log("------------Add New Binding :s-mts-group-2  Test Watch---------")
	binds = make([]*bind.Binding, 0, 2)
	binds = append(binds, bind.Bind_Direct("s-mts-group-2", "trade", "pay-succ", -1, true))
	etcdRegistry.PublishBindings("s-mts-group-2", binds)
	time.Sleep(5 * time.Second)

	bindings, err = etcdRegistry.GetBindAndWatch("trade")
	if nil != err {
		t.Error(err)
		t.Fail()
		return
	}
	if len(bindings) < 2 {
		t.Fail()
		t.Logf("TestEtcdSubscribe_QServer|GetQServerAndWatch|FAIL|%s\n", bindings)
		return
	}

	t.Logf("TestEtcdSubscribe_QServer|GetQServerAndWatch|SUCC|%v\n", bindings)
	time.Sleep(5 * time.Second)
}
