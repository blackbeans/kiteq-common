package registry

import (
	"github.com/blackbeans/kiteq-common/registry/bind"
	log "github.com/blackbeans/log4go"
	"github.com/coreos/etcd/client"
	"golang.org/x/net/context"
	"time"
)

type QServerWroker struct {
	Api             client.KeysAPI
	Hostport        string
	Topics          []string
	KeepalivePeriod time.Duration
}

//Keepalive with fixed period
func (self *QServerWroker) Keepalive() {
	for _, topic := range self.Topics {
		qpath := KITEQ_SERVER + "/" + topic + "/" + self.Hostport
		_, err := self.Api.Set(context.Background(),
			qpath, "", &client.SetOptions{Dir: true, TTL: self.KeepalivePeriod * 2})

		if nil != err {
			log.WarnLog("kiteq_registry", "QServerWroker|Keepalive|FAIL|%s|%s", qpath, err)
		}
	}
}

type QServersWatcher struct {
	Api     client.KeysAPI
	Topics  []string
	Watcher IWatcher
}

//watch QServer Change
func (self *QServersWatcher) Watch() {

	for _, topic := range self.Topics {

		//start watch topic server changed
		go func() {
			qpath := KITEQ_SERVER + "/" + topic
			w := self.Api.Watcher(qpath, &client.WatcherOptions{Recursive: true})
			for {
				resp, err := w.Next(context.Background())
				if nil != err {
					log.ErrorLog("kiteq_registry", "QServerWroker|Watch|FAIL|%s|%s", qpath, err)
					break
				}
				if resp.Action == "expire" || resp.Action == "delete" ||
					resp.Action == "create" {
					children := make([]string, resp.Node.Nodes.Len())
					now := time.Now()
					for _, n := range resp.Node.Nodes {
						if !n.Expiration.Before(now) {
							children = append(children)
						}
					}
					//push node children changed
					self.Watcher.NodeChange(qpath, Child, children)
					log.InfoLog("kiteq_registry", "QServerWroker|Watch|SUCC|%s|%v", qpath, children)
				}
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
		succpath, err := self.Api.Set(context.Background(), path, string(data),
			&client.SetOptions{Dir: false, TTL: self.KeepalivePeriod * 10})
		if nil != err {
			log.ErrorLog("kiteq_registry", "QClientWorker|Keepalive||Bind|FAIL|%s|%s/%s\n", err, path, binds)
			continue
		} else {
			log.InfoLog("kiteq_registry", "QClientWorker|Keepalive||Bind|SUCC|%s|%s\n", succpath, binds)
			continue
		}
	}
}

type BindWatcher struct {
	Api     client.KeysAPI
	Topics  []string
	Watcher IWatcher
}

//watch QServer Change
func (self *BindWatcher) Watch() {

	for _, topic := range self.Topics {

		//start watch topic server changed
		go func() {
			path := KITEQ_SUB + "/" + topic

			w := self.Api.Watcher(path, &client.WatcherOptions{Recursive: true})
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
					}
				}
			}
		}()
	}
}
