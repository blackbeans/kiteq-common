package binding

import (
	"testing"
	"time"
)

func TestPublishQServer(t *testing.T) {
	zkmanager := NewZKManager("localhost:2181")
	zkmanager.RegisteWather("/kiteq", &MockWatcher{})
	cleanUp(t, zkmanager, "/kiteq")

	topics := []string{"trade", "feed", "comment"}
	err := zkmanager.PublishQServer("localhost:13800", topics)
	if nil != err {
		t.Fail()
		t.Log(err)
		return
	}

	for _, topic := range topics {
		servers, err := zkmanager.GetQServerAndWatch(topic)
		if nil != err {
			t.Fail()
			t.Logf("%s|%s", err, topic)

		}
		if len(servers) != 1 {
			t.Fail()

		}
		t.Logf("TestPublishQServer|GetQServerAndWatch|%s|%s\n", topic, servers)
	}

	//主动删除一下
	cleanUp(t, zkmanager, "/kiteq")
	time.Sleep(10 * time.Second)
	zkmanager.Close()

}

func cleanUp(t *testing.T, zk *ZKManager, path string) {

	children, _, _ := zk.session.Children(path)

	//循环遍历当前孩子节点并删除
	for _, v := range children {
		tchildren, _, _ := zk.session.Children(path + "/" + v)
		if len(tchildren) <= 0 {
			//开始删除
			zk.session.Delete(path+"/"+v, -1)
			time.Sleep(2 * time.Second)
			t.Logf("cleanUp|%s\n", path+"/"+v)
		} else {
			cleanUp(t, zk, path+"/"+v)
		}
	}

	//删除当前节点
	zk.session.Delete(path, -1)
}

//测试发布 topic
func TestPublishTopic(t *testing.T) {

	topics := []string{"trade", "feed", "comment"}
	zkmanager := NewZKManager("localhost:2181")
	zkmanager.RegisteWather("/kiteq", &MockWatcher{})
	cleanUp(t, zkmanager, "/kiteq")

	err := zkmanager.PublishTopics(topics, "p-trade-a", "localhost:2181")
	if nil != err {
		t.Fail()
		t.Logf("TestPublishTopic|PublishTopic|%t|%s\n", topics, "localhost:2181")
		return
	}
	cleanUp(t, zkmanager, "/kiteq")
	zkmanager.Close()
}

//测试订阅topic
func TestSubscribeTopic(t *testing.T) {

	zkmanager := NewZKManager("localhost:2181")
	zkmanager.RegisteWather("/kiteq", &MockWatcher{})
	cleanUp(t, zkmanager, "/kiteq")

	persistentBind := []*Binding{Bind_Direct("s-trade-g", "trade", "trade-succ", -1, true)}
	tmpBind := []*Binding{Bind_Direct("s-trade-g", "trade-temp", "trade-fail", -1, false)}

	err := zkmanager.PublishBindings("s-trade-g", persistentBind)
	if nil != err {
		t.Fail()
		t.Logf("TestSubscribeTopic|SubscribeTopic|%s|%t\n", err, persistentBind)
		return
	}

	t.Logf("TestSubscribeTopic|SubscribeTopic|P|SUCC|%t\n", persistentBind)

	err = zkmanager.PublishBindings("s-trade-g", tmpBind)
	if nil != err {
		t.Fail()
		t.Logf("TestSubscribeTopic|SubscribeTopic|%t|%s\n", err, tmpBind)
	}

	t.Logf("TestSubscribeTopic|SubscribeTopic|T|SUCC|%s\n", tmpBind)

	//休息一下等待节点创建成功
	time.Sleep(1 * time.Second)

	bindings, err := zkmanager.GetBindAndWatch("trade")
	if nil != err {
		t.Fail()
		t.Logf("TestSubscribeTopic|GetBindAndWatch|trade|FAIL|%t|%s\n", err, "trade")
		return
	}

	t.Logf("TestSubscribeTopic|GetBindAndWatch|trade|SUCC|%t\n", bindings)
	if len(bindings) != 1 {
		t.Fail()

	}
	_, ok := bindings["s-trade-g"]
	if !ok {
		t.Fail()
	}

	bindings, err = zkmanager.GetBindAndWatch("trade-temp")
	if nil != err {
		t.Fail()
		t.Logf("TestSubscribeTopic|GetBindAndWatch|trade-temp|FAIL|%t|%s\n", err, "trade-temp")

	}
	t.Logf("TestSubscribeTopic|GetBindAndWatch|trade-temp|SUCC|%t\n", bindings)

	if len(bindings) != 1 {
		t.Fail()

	}

	_, ok = bindings["s-trade-g"]
	if !ok {
		t.Fail()
	}

	cleanUp(t, zkmanager, "/kiteq")

	time.Sleep(10 * time.Second)
	zkmanager.Close()
}
