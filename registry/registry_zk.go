package registry

import (
	"github.com/blackbeans/go-zookeeper/zk"
	log "github.com/sirupsen/logrus"
	_ "net"
	"strings"
	"time"
)

type ZKManager struct {
	zkhosts   string
	watchers  []IWatcher //基本的路径--->watcher zk可以复用了
	session   *zk.Conn
	eventChan <-chan zk.Event
	isClose   bool
}

func NewZKManager(zkhosts string) *ZKManager {
	zkmanager := &ZKManager{zkhosts: zkhosts, watchers: []IWatcher{}}
	return zkmanager
}

func (z *ZKManager) Start() {
	if len(z.zkhosts) <= 0 {
		log.Warn("使用默认zkhosts！|localhost:2181")
		z.zkhosts = "localhost:2181"
	} else {
		log.Infof("使用zkhosts:[%s]!", z.zkhosts)
	}

	ss, eventChan, err := zk.Connect(strings.Split(z.zkhosts, ","), 5*time.Second)
	if nil != err {
		panic("连接zk失败..." + err.Error())
		return
	}

	exist, _, err := ss.Exists(KITEQ)
	if nil != err {
		ss.Close()
		panic("无法创建KITEQ " + err.Error())

	}

	if !exist {
		resp, err := ss.Create(KITEQ, nil, zk.CreatePersistent, zk.WorldACL(zk.PermAll))
		if nil != err {
			ss.Close()
			panic("NewZKManager|CREATE ROOT PATH|FAIL|" + KITEQ + "|" + err.Error())
		} else {
			log.Infof("NewZKManager|CREATE ROOT PATH|SUCC|%s", resp)
		}
	}

	z.session = ss
	z.isClose = false
	z.eventChan = eventChan
	go z.listenEvent()
}

//如果返回false则已经存在
func (z *ZKManager) RegisterWatcher(w IWatcher) bool {

	z.watchers = append(z.watchers, w)
	return true
}

//监听数据变更
func (z *ZKManager) listenEvent() {
	for !z.isClose {

		//根据zk的文档 Watcher机制是无法保证可靠的，其次需要在每次处理完Watcher后要重新注册Watcher
		change := <-z.eventChan
		path := change.Path

		switch change.Type {
		case zk.EventSession:
			if change.State == zk.StateExpired ||
				change.State == zk.StateDisconnected {
				log.Warnf("ZKManager|OnSessionExpired!|Reconnect Zk ....")

				//session失效必须通知所有的watcher
				for _, w := range z.watchers {
					//zk链接开则需要重新链接重新推送
					w.OnSessionExpired()
				}

			}
		case zk.EventNodeDeleted:
			z.session.ExistsW(path)
			z.NodeChange(path, RegistryEvent(change.Type), []string{})
			// log.Info("ZKManager|listenEvent|%s|%s", path, change)
		case zk.EventNodeCreated, zk.EventNodeChildrenChanged:
			childnodes, _, _, err := z.session.ChildrenW(path)
			if nil != err {
				log.Errorf("ZKManager|listenEvent|CD|%s|%s|%v", err, path, change.Type)
			} else {
				z.NodeChange(path, RegistryEvent(change.Type), childnodes)
				// log.Info("ZKManager|listenEvent|%s|%s|%s", path, change, childnodes)
			}

		case zk.EventNodeDataChanged:
			split := strings.Split(path, "/")
			//如果不是bind级别的变更则忽略
			if len(split) < 5 || strings.LastIndex(split[4], "-bind") <= 0 {
				continue
			}
			//获取一下数据
			binds, err := z.getBindData(path)
			if nil != err {
				log.Errorf("ZKManager|listenEvent|Changed|Get DATA|FAIL|%s|%s", err, path)
				//忽略
				continue
			}
			z.DataChange(path, binds)
			// log.Info("ZKManager|listenEvent|%s|%s|%s", path, change, binds)

		}

	}

}

//去除掉当前的KiteQServer
func (z *ZKManager) UnPublishQServer(hostport string, topics []string) {
	for _, topic := range topics {

		qpath := KITEQ_SERVER + "/" + topic + "/" + hostport

		//删除当前该Topic下的本机
		err := z.session.Delete(qpath, -1)
		if nil != err {
			log.Errorf("ZKManager|UnpushlishQServer|FAIL|%s|%s", err, qpath)
		} else {
			log.Infof("ZKManager|UnpushlishQServer|SUCC|%s", qpath)
		}
	}
	//注册当前的kiteqserver
	z.session.Delete(KITEQ_ALIVE_SERVERS+"/"+hostport, -1)
}

//发布topic对应的server
func (z *ZKManager) PublishQServer(hostport string, topics []string) error {

	for _, topic := range topics {
		qpath := KITEQ_SERVER + "/" + topic
		spath := KITEQ_SUB + "/" + topic
		ppath := KITEQ_PUB + "/" + topic

		//创建发送和订阅的根节点
		z.traverseCreatePath(ppath, nil, zk.CreatePersistent)
		// z.session.ExistsW(ppath)
		z.traverseCreatePath(spath, nil, zk.CreatePersistent)
		z.session.ExistsW(spath)

		//先删除当前这个临时节点再注册 避免监听不到临时节点变更的事件
		z.session.Delete(qpath+"/"+hostport, -1)

		//注册当前节点
		path, err := z.registerPath(qpath, hostport, zk.CreateEphemeral, nil)
		if nil != err {
			log.Errorf("ZKManager|PublishQServer|FAIL|%s|%s/%s", err, qpath, hostport)
			return err
		}
		log.Infof("ZKManager|PublishQServer|SUCC|%s", path)
	}

	//注册当前的kiteqserver
	z.session.Delete(KITEQ_ALIVE_SERVERS+"/"+hostport, -1)
	z.registerPath(KITEQ_ALIVE_SERVERS, hostport, zk.CreateEphemeral, nil)
	z.registerPath(KITEQ_ALL_SERVERS, hostport, zk.CreatePersistent, nil)
	return nil
}

//发布可以使用的topic类型的publisher
func (z *ZKManager) PublishTopics(topics []string, groupId string, hostport string) error {

	for _, topic := range topics {
		pubPath := KITEQ_PUB + "/" + topic + "/" + groupId
		path, err := z.registerPath(pubPath, hostport, zk.CreatePersistent, nil)
		if nil != err {
			log.Errorf("ZKManager|PublishTopic|FAIL|%s|%s/%s", err, pubPath, hostport)
			return err
		}
		log.Infof("ZKManager|PublishTopic|SUCC|%s", path)
	}
	return nil
}

//发布订阅关系
func (z *ZKManager) PublishBindings(groupId string, bindings []*Binding) error {

	//按topic分组
	groupBind := make(map[string][]*Binding, 10)
	for _, b := range bindings {
		g, ok := groupBind[b.Topic]
		if !ok {
			g = make([]*Binding, 0, 2)
		}
		b.GroupId = groupId
		g = append(g, b)
		groupBind[b.Topic] = g
	}

	for topic, binds := range groupBind {
		data, err := MarshalBinds(binds)
		if nil != err {
			log.Errorf("ZKManager|PublishBindings|MarshalBind|FAIL|%s|%s|%v", err, groupId, binds)
			return err
		}

		createType := zk.CreatePersistent

		path := KITEQ_SUB + "/" + topic
		//注册对应topic的groupId //注册订阅信息
		succpath, err := z.registerPath(path, groupId+"-bind", createType, data)
		if nil != err {
			log.Errorf("ZKManager|PublishTopic|Bind|FAIL|%s|%s/%v", err, path, binds)
			return err
		} else {
			log.Infof("ZKManager|PublishTopic|Bind|SUCC|%s|%v", succpath, binds)
		}
	}
	return nil
}

//注册当前进程节点
func (z *ZKManager) registerPath(path string, childpath string, createType zk.CreateType, data []byte) (string, error) {
	err := z.traverseCreatePath(path, nil, zk.CreatePersistent)
	if nil == err {
		err := z.innerCreatePath(path+"/"+childpath, data, createType)
		if nil != err {
			log.Errorf("ZKManager|registerPath|CREATE CHILD|FAIL|%s|%s", err, path+"/"+childpath)
			return "", err
		} else {
			return path + "/" + childpath, nil
		}
	}
	return "", err

}

func (z *ZKManager) traverseCreatePath(path string, data []byte, createType zk.CreateType) error {
	split := strings.Split(path, "/")[1:]
	tmppath := "/"
	for i, v := range split {
		tmppath += v
		// log.Printf("ZKManager|traverseCreatePath|%s", tmppath)
		if i >= len(split)-1 {
			break
		}
		err := z.innerCreatePath(tmppath, nil, zk.CreatePersistent)
		if nil != err {
			log.Errorf("ZKManager|traverseCreatePath|FAIL|%s", err)
			return err
		}
		tmppath += "/"

	}

	//对叶子节点创建及添加数据
	return z.innerCreatePath(tmppath, data, createType)
}

//内部创建节点的方法
func (z *ZKManager) innerCreatePath(tmppath string, data []byte, createType zk.CreateType) error {
	exist, _, _, err := z.session.ExistsW(tmppath)
	if nil == err && !exist {
		_, err := z.session.Create(tmppath, data, createType, zk.WorldACL(zk.PermAll))
		if nil != err {
			log.Errorf("ZKManager|innerCreatePath|FAIL|%v|%s", err, tmppath)
			return err
		}

		//做一下校验等待
		for i := 0; i < 5; i++ {
			exist, _, _ = z.session.Exists(tmppath)
			if !exist {
				time.Sleep(time.Duration(i*100) * time.Millisecond)
			} else {
				break
			}
		}

		return err
	} else if nil != err {
		log.Errorf("ZKManager|innerCreatePath|FAIL|%s", err)
		return err
	} else if nil != data {
		//存在该节点，推送新数据
		_, err := z.session.Set(tmppath, data, -1)
		if nil != err {
			log.Errorf("ZKManager|innerCreatePath|PUSH DATA|FAIL|%v|%s|%s", err, tmppath, string(data))
			return err
		}
	}
	return nil
}

//获取QServer并添加watcher
func (z *ZKManager) GetQServerAndWatch(topic string) ([]string, error) {

	path := KITEQ_SERVER + "/" + topic

	exist, _, _, _ := z.session.ExistsW(path)
	if !exist {
		return []string{}, nil
	}
	//获取topic下的所有qserver
	children, _, _, err := z.session.ChildrenW(path)
	if nil != err {
		log.Errorf("ZKManager|GetQServerAndWatch|FAIL|%v|%s", err, path)
		return nil, err
	}
	return children, nil
}

//获取订阅关系并添加watcher
func (z *ZKManager) GetBindAndWatch(topic string) (map[string][]*Binding, error) {

	path := KITEQ_SUB + "/" + topic

	exist, _, _, err := z.session.ExistsW(path)
	if !exist {
		//不存在订阅关系的时候需要创建该topic和
		return make(map[string][]*Binding, 0), err
	}

	//获取topic下的所有qserver
	groupIds, _, _, err := z.session.ChildrenW(path)
	if nil != err {
		log.Errorf("ZKManager|GetBindAndWatch|GroupID|FAIL|%v|%s", err, path)
		return nil, err
	}

	hps := make(map[string][]*Binding, len(groupIds))
	//获取topic对应的所有groupId下的订阅关系
	for _, groupId := range groupIds {
		tmppath := path + "/" + groupId
		binds, err := z.getBindData(tmppath)
		if nil != err {
			log.Errorf("GetBindAndWatch|getBindData|FAIL|%v|%s", err, tmppath)
			continue
		}

		//去掉分组后面的-bind
		gid := strings.TrimSuffix(groupId, "-bind")
		hps[gid] = binds
	}

	return hps, nil
}

//获取绑定对象的数据
func (z *ZKManager) getBindData(path string) ([]*Binding, error) {
	bindData, _, _, err := z.session.GetW(path)
	if nil != err {
		log.Errorf("ZKManager|getBindData|Binding|FAIL|%v|%s", err, path)
		return nil, err
	}

	// log.Printf("ZKManager|getBindData|Binding|SUCC|%s|%s", path, string(bindData))
	if nil == bindData || len(bindData) <= 0 {
		return []*Binding{}, nil
	} else {
		binding, err := UmarshalBinds(bindData)
		if nil != err {
			log.Errorf("ZKManager|getBindData|UmarshalBind|FAIL|%v|%s|%s", err, path, string(bindData))

		}
		return binding, err
	}

}

func (z *ZKManager) Close() {
	z.isClose = true
	z.session.Close()
}

//订阅关系topic下的group发生变更
func (z *ZKManager) NodeChange(path string, eventType RegistryEvent, childNode []string) {

	//如果是订阅关系变更则处理
	if strings.HasPrefix(path, KITEQ_SUB) {
		//获取topic
		split := strings.Split(path, "/")
		if len(split) < 4 {
			if eventType == Created {
				//不合法的订阅璐姐
				log.Errorf("ZKManager|NodeChange|INVALID SUB PATH |%s|%t", path, childNode)
			}
			return
		}
		//获取topic
		topic := split[3]

		//如果topic下无订阅分组节点，直接删除该topic
		if len(childNode) <= 0 {
			for _, w := range z.watchers {
				w.OnBindChanged(topic, "", nil)
			}
			log.Errorf("ZKManager|NodeChange|无子节点|%s|%s", path, childNode)
			return
		}

		// //对当前的topic的分组进行重新设置
		switch eventType {
		case Created, Child:

			bm, err := z.GetBindAndWatch(topic)
			if nil != err {
				log.Errorf("ZKManager|NodeChange|获取订阅关系失败|%s|%s", path, childNode)
			}

			//如果topic下没有订阅关系分组则青琉璃
			if len(bm) > 0 {
				for groupId, bs := range bm {
					for _, w := range z.watchers {
						w.OnBindChanged(topic, groupId, bs)
					}
				}
			} else {
				//删除具体某个分组
				for _, w := range z.watchers {
					w.OnBindChanged(topic, "", nil)
				}
			}
		}

	} else {
		// log.Warn("BindExchanger|NodeChange|非SUB节点变更|%s|%s", path, childNode)
	}
}

//数据变更
func (z *ZKManager) DataChange(path string, binds []*Binding) {

	//订阅关系变更才处理
	if strings.HasPrefix(path, KITEQ_SUB) {

		split := strings.Split(path, "/")
		//获取topic
		topic := split[3]
		groupId := strings.TrimSuffix(split[4], "-bind")
		for _, w := range z.watchers {
			//zk链接开则需要重新链接重新推送
			//开始处理变化的订阅关系
			w.OnBindChanged(topic, groupId, binds)
		}
	} else {
		log.Warnf("ZKManager|DataChange|非SUB节点变更|%s", path)
	}

}
