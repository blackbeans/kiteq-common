package registry

import (
	log "github.com/sirupsen/logrus"
	"strings"
)

const (
	KITEQ               = "/kiteq"
	KITEQ_ALL_SERVERS   = KITEQ + "/all_servers"
	KITEQ_ALIVE_SERVERS = KITEQ + "/alive_servers"
	KITEQ_SERVER        = KITEQ + "/server" // 临时节点 # /kiteq/server/${topic}/ip:port
	KITEQ_PUB           = KITEQ + "/pub"    // 临时节点 # /kiteq/pub/${topic}/${groupId}/ip:port
	KITEQ_SUB           = KITEQ + "/sub"    // 持久订阅/或者临时订阅 # /kiteq/sub/${topic}/${groupId}-bind/#$data(bind)
)

type RegistryEvent byte

const (
	Created RegistryEvent = 1 // From Exists, Get NodeCreated (1),
	Deleted RegistryEvent = 2 // From Exists, Get	NodeDeleted (2),
	Changed RegistryEvent = 3 // From Exists, Get NodeDataChanged (3),
	Child   RegistryEvent = 4 // From Children NodeChildrenChanged (4)
)

//每个watcher
type IWatcher interface {
	//当断开链接时
	OnSessionExpired()

	DataChange(path string, binds []*Binding)
	NodeChange(path string, eventType RegistryEvent, children []string)
}

type Registry interface {
	//启动
	Start()

	//如果返回false则已经存在
	RegisterWatcher(rootpath string, w IWatcher) bool

	//去除掉当前的KiteQServer
	UnPublishQServer(hostport string, topics []string)

	//发布topic对应的server
	PublishQServer(hostport string, topics []string) error

	//发布可以使用的topic类型的publisher
	PublishTopics(topics []string, groupId string, hostport string) error

	//发布订阅关系
	PublishBindings(groupId string, bindings []*Binding) error

	//获取QServer并添加watcher
	GetQServerAndWatch(topic string) ([]string, error)

	//获取订阅关系并添加watcher
	GetBindAndWatch(topic string) (map[string][]*Binding, error)

	Close()
}

type MockWatcher struct {
}

func (self *MockWatcher) OnSessionExpired() {

}

func (self *MockWatcher) DataChange(path string, binds []*Binding) {

	//订阅关系变更才处理
	if strings.HasPrefix(path, KITEQ_SUB) {

		split := strings.Split(path, "/")
		//如果不是bind级别的变更则忽略
		if len(split) < 5 || strings.LastIndex(split[4], "-bind") <= 0 {
			return
		}

		//开始处理变化的订阅关系
		log.Infof("MockWatcher|DataChange|SUB节点变更|%s|%v", path, binds)
	} else {
		log.Infof("MockWatcher|DataChange|非SUB节点变更|%s", path)
	}

	return
}

//订阅关系topic下的group发生变更
func (self *MockWatcher) NodeChange(path string, eventType RegistryEvent, childNode []string) {

	//如果是订阅关系变更则处理
	if strings.HasPrefix(path, KITEQ_SUB) {
		//获取topic
		split := strings.Split(path, "/")
		if len(split) < 4 {
			//不合法的订阅璐姐
			log.Infof("MockWatcher|NodeChange|INVALID SUB PATH |%s|%d|%v", path, eventType, childNode)
			return
		}
		log.Infof("MockWatcher|NodeChange|SUB节点变更||%s|%d|%v", path, eventType, childNode)
	} else {
		log.Infof("MockWatcher|NodeChange|节点变更||%s|%d|%v", path, eventType, childNode)
	}
	return
}
