package registry

import (
	"context"
	"github.com/blackbeans/logx"
	"strings"
)

var log = logx.GetLogger("kiteq_registry")

type RegistryCenter struct {
	registry Registry
}

//uri:
//	zk://localhost:2181,localhost:2181?timeout=50s
//  file://./conf/registry_demo.yaml

func NewRegistryCenter(parent context.Context, uri string) *RegistryCenter {
	var registry Registry
	startIdx := strings.Index(uri, "://")
	schema := uri[0:startIdx]

	//zk
	if "zk" == schema {
		p := strings.Split(uri[startIdx+3:], "?")
		hosts := p[0]
		if len(p) > 1 {
			data := strings.Split(p[1], "&")
			params := make(map[string]string, len(data)+1)
			for _, v := range data {
				p := strings.SplitN(v, "=", 2)
				if len(p) >= 2 {
					params[p[0]] = p[1]
				}
			}
		}

		if len(hosts) > 0 {
			registry = NewZKManager(hosts)
		}

	} else if "file" == schema {
		//本地文件配置
		registry = NewFileRegistry(parent, strings.TrimPrefix(uri, "file://"))

	} else {
		panic("Unsupport Registry [" + uri + "]")
	}

	//start registry
	registry.Start()

	center := &RegistryCenter{registry: registry}
	return center

}

//如果返回false则已经存在
func (self *RegistryCenter) RegisterWatcher(w IWatcher) bool {
	return self.registry.RegisterWatcher(w)
}

//去除掉当前的KiteQServer
func (self *RegistryCenter) UnPublishQServer(hostport string, topics []string) {
	self.registry.UnPublishQServer(hostport, topics)
}

//发布topic对应的server
func (self *RegistryCenter) PublishQServer(hostport string, topics []string) error {
	return self.registry.PublishQServer(hostport, topics)
}

//发布可以使用的topic类型的publisher
func (self *RegistryCenter) PublishTopics(topics []string, groupId string, hostport string) error {
	return self.registry.PublishTopics(topics, groupId, hostport)
}

//发布订阅关系
func (self *RegistryCenter) PublishBindings(groupId string, bindings []*Binding) error {
	return self.registry.PublishBindings(groupId, bindings)
}

//获取QServer并添加watcher
func (self *RegistryCenter) GetQServerAndWatch(topic string) ([]string, error) {
	return self.registry.GetQServerAndWatch(topic)
}

//获取订阅关系并添加watcher
func (self *RegistryCenter) GetBindAndWatch(topic string) (map[string][]*Binding, error) {
	return self.registry.GetBindAndWatch(topic)
}

func (self *RegistryCenter) Close() {
	self.registry.Close()
}
