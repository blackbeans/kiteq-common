package registry

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
	//订阅关系变更
	OnBindChanged(topic, groupId string, newbinds []*Binding)

	//当QServer变更
	OnQServerChanged(topic string, hosts []string)

	//当断开链接时
	OnSessionExpired()
}

type Registry interface {
	//启动
	Start()

	//如果返回false则已经存在
	RegisterWatcher(w IWatcher) bool

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

func (self *MockWatcher) OnBindChanged(topic, groupId string, newbinds []*Binding) {

}

func (m *MockWatcher) OnQServerChanged(topic string, hosts []string) {

}
