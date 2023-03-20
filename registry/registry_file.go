package registry

import (
	"context"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"sort"
	"time"
)

//broker的配置
type Broker struct {
	Topics  []string `yaml:"topics"`
	Address string   `yaml:"address"`
	Env     string   `yaml:"env"`
}

//文件的bingding
type FileBinding struct {
	GroupIds    []string `yaml:"groupIds"`
	Topic       string   `yaml:"topic"`
	MessageType string   `yaml:"messageType"`
	BindType    string   `yaml:"bindType"`
	Watermark   int32    `yaml:"watermark"`
	Persistent  bool     `yaml:"persistent"`
}

//文件注册的信息
type FileRegistryInfo struct {
	Brokers  []*Broker      `yaml:"brokers"`
	Bindings []*FileBinding `yaml:"bindings"`
}

//文件的配置的 registry
type FileRegistry struct {
	Registry
	ctx          context.Context
	cancel       context.CancelFunc
	path         string
	registryInfo FileRegistryInfo
}

func NewFileRegistry(parent context.Context, path string) *FileRegistry {
	ctx, cancel := context.WithCancel(parent)
	return &FileRegistry{ctx: ctx, cancel: cancel, path: path}
}

func (f *FileRegistry) Start() {
	f.reloadBrokers()
	go func() {
		for {
			time.Sleep(time.Minute)
			select {
			case <-f.ctx.Done():
				return
			default:
				//定期加载
				f.reloadBrokers()
			}
		}
	}()

}

//加载bokers
func (f *FileRegistry) reloadBrokers() {
	//加载本地配置文件中的配置
	raw, err := ioutil.ReadFile(f.path)
	if nil != err {
		panic(err)
	}
	//brokers
	var registryInfo FileRegistryInfo
	if err = yaml.Unmarshal(raw, &registryInfo); nil != err {
		panic(err)
	}

	uniqBrokers := make(map[string]interface{}, 10)
	//检查所有的节点配置
	for _, broker := range registryInfo.Brokers {
		_, ok := uniqBrokers[broker.Address]
		if ok {
			//配置冲突了
			log.Warnf("FileRegistry|reloadBrokers|Broker Conflict ! %s", broker.Address)
			return
		}
		uniqBrokers[broker.Address] = nil
	}
	//当前加载broker配置
	f.registryInfo = registryInfo
	log.Infof("FileRegistry|Brokers:%v", f.registryInfo)
}

func (f *FileRegistry) RegisterWatcher(rootpath string, w IWatcher) bool {
	//TODO 是否要做监听准备
	return true
}

func (f *FileRegistry) UnPublishQServer(hostport string, topics []string) {
	log.Infof("FileRegistry|UnPublishQServer|%s|%s", hostport, topics)
}

func (f *FileRegistry) PublishQServer(hostport string, topics []string) error {
	log.Infof("FileRegistry|PublishQServer|%s|%s", hostport, topics)
	return nil
}

func (f *FileRegistry) PublishTopics(topics []string, groupId string, hostport string) error {
	log.Infof("FileRegistry|PublishTopics|%s|%s|%s", topics, groupId, hostport)
	return nil
}

func (f *FileRegistry) PublishBindings(groupId string, bindings []*Binding) error {
	log.Infof("FileRegistry|PublishBindings|%s|%v", groupId, bindings)
	return nil
}

func (f *FileRegistry) GetQServerAndWatch(topic string) ([]string, error) {
	brokers := make([]string, 0, 10)
	for _, broker := range f.registryInfo.Brokers {
		idx := sort.SearchStrings(broker.Topics, topic)
		if idx != len(broker.Topics) && broker.Topics[idx] == topic {
			brokers = append(brokers, broker.Address)
		}
	}
	return brokers, nil
}

//获取分组对应的topic的订阅关系
func (f *FileRegistry) GetBindAndWatch(topic string) (map[string][]*Binding, error) {
	bindings := make(map[string][]*Binding, 10)
	for _, bind := range f.registryInfo.Bindings {
		if bind.Topic == topic {
			for _, gid := range bind.GroupIds {
				if _, ok := bindings[gid]; !ok {
					bindings[gid] = make([]*Binding, 0, 2)
				}
				bindings[gid] = append(bindings[gid], binding(gid, topic, bind.MessageType, TypeOfBind(bind.BindType), bind.Watermark, bind.Persistent))
			}
		}
	}
	return bindings, nil
}

func (f *FileRegistry) Close() {
	f.cancel()
}
