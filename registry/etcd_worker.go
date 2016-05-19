package registry

import (
	"github.com/blackbeans/kiteq-common/registry/bind"
	log "github.com/blackbeans/log4go"
	"github.com/coreos/etcd/client"
	"golang.org/x/net/context"
	"strings"
	"sync"
	"time"
)

type Worker interface {
	Start()
	Keepalive()
}

type EtcdWatcher interface {
	Watch()
}

type QServerWroker struct {
	Api             client.KeysAPI
	Hostport        string
	Topics          []string
	KeepalivePeriod time.Duration
}

func (self *QServerWroker) Start() {
	for _, topic := range self.Topics {
		qpath := KITEQ_SERVER + "/" + topic + "/" + self.Hostport
		self.Api.Delete(context.Background(), qpath, &client.DeleteOptions{Recursive: true})
		_, err := self.Api.Set(context.Background(), qpath, "", &client.SetOptions{TTL: 2 * self.KeepalivePeriod, Dir: true})
		if nil != err {
			log.ErrorLog("kiteq_registry", "QServerWroker|Keepalive|Start|FAIL|%s|%s", qpath, err)
		} else {
			log.InfoLog("kiteq_registry", "QServerWroker|Keepalive|Start|SUCC|%s", qpath)
		}
	}
	//先移除
	self.Api.Delete(context.Background(), KITEQ_ALL_SERVERS+"/"+self.Hostport, &client.DeleteOptions{Recursive: true})
	self.Api.Delete(context.Background(), KITEQ_ALIVE_SERVERS+"/"+self.Hostport, &client.DeleteOptions{Recursive: true})

	//create kiteqserver
	self.Api.Create(context.Background(), KITEQ_ALIVE_SERVERS+"/"+self.Hostport, "")
	self.Api.Create(context.Background(), KITEQ_ALL_SERVERS+"/"+self.Hostport, "")
}

//Keepalive with fixed period
func (self *QServerWroker) Keepalive() {
	op := &client.SetOptions{Dir: true, TTL: 2 * self.KeepalivePeriod}
	for _, topic := range self.Topics {
		qpath := KITEQ_SERVER + "/" + topic + "/" + self.Hostport
		_, err := self.Api.Set(context.Background(), qpath, "", op)
		if nil != err {
			log.ErrorLog("kiteq_registry", "QServerWroker|Keepalive|FAIL|%s|%s", qpath, err)
		}
	}

	//keepalive alive server
	self.Api.Set(context.Background(), KITEQ_ALIVE_SERVERS+"/"+self.Hostport, "", op)
	self.Api.Set(context.Background(), KITEQ_ALL_SERVERS+"/"+self.Hostport, "", op)
}

type QServersWatcher struct {
	Api          client.KeysAPI
	Topics       []string
	Watcher      IWatcher
	topic2Server map[string] /*topic*/ []string
	lock         sync.Mutex
}

//watch QServer Change
func (self *QServersWatcher) Watch() {

	self.topic2Server = make(map[string][]string, 10)
	for _, topic := range self.Topics {
		qpath := KITEQ_SERVER + "/" + topic
		tmpTopic := topic
		//start watch topic server changed
		go func() {
			w := self.Api.Watcher(qpath, &client.WatcherOptions{Recursive: false})
			for {
				resp, err := w.Next(context.Background())
				if nil != err {
					log.ErrorLog("kiteq_registry", "QServerWroker|Watch|FAIL|%s|%s", qpath, err)
					break
				}

				self.lock.Lock()
				//check local data
				servers, ok := self.topic2Server[tmpTopic]
				if !ok {
					servers = make([]string, 0, 10)
				}

				ipport := resp.Node.Key[strings.LastIndex(resp.Node.Key, "/")+1:]
				existIdx := -1
				for i, s := range servers {
					if s == ipport {
						existIdx = i
						break
					}
				}

				if resp.Action == "create" {
					if existIdx < 0 {
						servers = append(servers, ipport)
						//push node children changed
						self.Watcher.NodeChange(qpath, Child, servers)
						log.InfoLog("kiteq_registry", "QServerWroker|Watch|SUCC|%s|%s|%v", resp.Action, qpath, servers)
					}

				} else if resp.Action == "delete " || resp.Action == "expire" {
					//delete server
					if existIdx >= 0 {
						servers = append(servers[0:existIdx], servers[existIdx+1:]...)
						self.Watcher.NodeChange(qpath, Child, servers)
						log.InfoLog("kiteq_registry", "QServerWroker|Watch|SUCC|%s|%s|%v", resp.Action, qpath, servers)
					}
				}

				self.topic2Server[tmpTopic] = servers
				self.lock.Unlock()
			}
		}()
	}
}

//qclient workder
type QClientWorker struct {
	Api             client.KeysAPI
	PublishTopics   []string
	Hostport        string
	GroupId         string
	Bindings        []*bind.Binding
	KeepalivePeriod time.Duration
}

func (self *QClientWorker) Start() {

	//create root
	_, err := self.Api.Set(context.Background(), KITEQ_PUB, "", &client.SetOptions{Dir: true, TTL: self.KeepalivePeriod * 2})
	if nil != err {
		log.ErrorLog("kiteq_registry", "QClientWorker|Start|FAIL|%s|%s", KITEQ_PUB, err)
	}
	_, err = self.Api.Set(context.Background(), KITEQ_SUB, "", &client.SetOptions{Dir: true, TTL: -1})
	if nil != err {
		log.ErrorLog("kiteq_registry", "QClientWorker|Start|FAIL|%s|%s", KITEQ_SUB, err)
	}
	//init
	self.Keepalive()
}

//Keepalive with fixed period
func (self *QClientWorker) Keepalive() {
	//PublishTopics
	for _, topic := range self.PublishTopics {

		pubPath := KITEQ_PUB + "/" + topic + "/" + self.GroupId + "/" + self.Hostport
		_, err := self.Api.Set(context.Background(),
			pubPath, "", &client.SetOptions{Dir: true, TTL: self.KeepalivePeriod * 2})

		if nil != err {
			log.WarnLog("kiteq_registry", "QClientWorker|Keepalive|FAIL|%s|%s", pubPath, err)
		}
	}
	//按topic分组
	groupBind := make(map[string][]*bind.Binding, 10)
	for _, b := range self.Bindings {
		g, ok := groupBind[b.Topic]
		if !ok {
			g = make([]*bind.Binding, 0, 2)
		}
		b.GroupId = self.GroupId
		g = append(g, b)
		groupBind[b.Topic] = g
	}

	for topic, binds := range groupBind {
		data, err := bind.MarshalBinds(binds)
		if nil != err {
			log.ErrorLog("kiteq_registry", "QClientWorker|Keepalive|MarshalBind|FAIL|%s|%s|%v\n", err, self.GroupId, binds)
			continue
		}

		path := KITEQ_SUB + "/" + topic + "/" + self.GroupId + "-bind"
		//注册对应topic的groupId //注册订阅信息
		_, err = self.Api.Set(context.Background(), path, string(data),
			&client.SetOptions{Dir: false, TTL: self.KeepalivePeriod * 10})
		if nil != err {
			log.ErrorLog("kiteq_registry", "QClientWorker|Keepalive||Bind|FAIL|%s|%s|%v\n", err, path, binds)
		} else {
			// log.InfoLog("kiteq_registry", "QClientWorker|Keepalive||Bind|SUCC|%s|%v\n", succpath, binds)
		}
	}
}

type BindWatcher struct {
	Api     client.KeysAPI
	Topics  []string
	Watcher IWatcher
	binds   map[string] /*topic*/ map[string] /*groupId*/ []bind.Binding
}

//watch QServer Change
func (self *BindWatcher) Watch() {

	self.binds = make(map[string]map[string][]bind.Binding, 5)

	for _, topic := range self.Topics {
		path := KITEQ_SUB + "/" + topic
		//start watch topic server changed
		go func() {
			w := self.Api.Watcher(path, &client.WatcherOptions{Recursive: false})
			for {
				resp, err := w.Next(context.Background())
				if nil != err {
					log.ErrorLog("kiteq_registry", "BindWatcher|Watch|FAIL|%s|%s", path, err)
					break
				}

				if resp.Action == "delete" || resp.Action == "expire" {
					//push bind data changed
					children := make([]string, 0, resp.Node.Nodes.Len())
					now := time.Now()
					for _, n := range resp.Node.Nodes {
						if !n.Expiration.Before(now) {
							children = append(children)
						}
					}

					self.Watcher.NodeChange(path, Child, children)
					log.InfoLog("kiteq_registry", "BindWatcher|Watch|Bind|SUCC|%s|%v", path, children)
				} else if resp.Action == "set" || resp.Action == "update" {
					//bind data changed
					for _, n := range resp.Node.Nodes {
						w := self.Api.Watcher(path+"/"+n.Key, &client.WatcherOptions{Recursive: false})
						//watch
						go func() {
							for {
								resp, err := w.Next(context.Background())
								if nil != err {
									log.ErrorLog("kiteq_registry", "BindWatcher|Watch|BindFAIL|%s|%s", path+"/"+n.Key, err)
									break
								}
								if resp.Action == "set" || resp.Action == "update" {
									binds, _ := bind.UmarshalBinds([]byte(resp.Node.Value))
									//push bind data changed
									self.Watcher.DataChange(path+"/"+n.Key, binds)
									log.InfoLog("kiteq_registry", "BindWatcher|Watch|Bind|SUCC|%s|%v", path+"/"+n.Key, binds)
								}
							}
						}()
					}
				}
			}
		}()
	}
}
