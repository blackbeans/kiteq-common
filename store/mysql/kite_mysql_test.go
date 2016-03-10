package mysql

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"kiteq/protocol"
	"kiteq/store"
	"log"
	"testing"
	"time"
)

func TestPageQuery(t *testing.T) {

	options := MysqlOptions{
		Addr:         "localhost:3306",
		DB:           "kite",
		Username:     "root",
		Password:     "",
		ShardNum:     4,
		BatchUpSize:  100,
		BatchDelSize: 100,
		FlushPeriod:  10 * time.Millisecond,
		MaxIdleConn:  10,
		MaxOpenConn:  10}

	kiteMysql := NewKiteMysql(options, "localhost")
	truncate(kiteMysql)
	hn := "localhost"
	for i := 0; i < 10; i++ {
		//创建消息
		msg := &protocol.BytesMessage{}
		msg.Header = &protocol.Header{
			MessageId:    proto.String(fmt.Sprintf("%x", i) + "26c03f00665862591f696a980b5ac"),
			Topic:        proto.String("trade"),
			MessageType:  proto.String("pay-succ"),
			ExpiredTime:  proto.Int64(time.Now().Add(10 * time.Minute).Unix()),
			DeliverLimit: proto.Int32(100),
			GroupId:      proto.String("go-kite-test"),
			Commit:       proto.Bool(false),
			Fly:          proto.Bool(false)}
		msg.Body = []byte("hello world")

		entity := store.NewMessageEntity(protocol.NewQMessage(msg))

		entity.KiteServer = hn
		entity.PublishTime = time.Now().Unix()
		kiteMysql.Save(entity)
	}

	startIdx := 0
	hasMore := true
	count := 0
	//开始分页查询未过期的消息实体
	for hasMore {
		more, entities := kiteMysql.PageQueryEntity("c", hn,
			time.Now().Unix(), 0, 1)
		if len(entities) <= 0 {
			break
		}

		//开始发起重投
		for _, entity := range entities {
			count++
			t.Logf("TestPageQuery|PageQueryEntity|%s\n", entity.MessageId)
			msg := &store.MessageEntity{
				MessageId:    entity.MessageId,
				DeliverCount: 1,
				SuccGroups:   []string{},
				FailGroups:   []string{"s-mts-test"},
				//设置一下下一次投递时间
				NextDeliverTime: time.Now().Add(1 * time.Minute).Unix()}
			kiteMysql.AsyncUpdate(msg)

		}

		time.Sleep(1 * time.Second)
		hasMore = more
		startIdx += len(entities)
	}
	if count != 10 {
		t.Fail()
		t.Logf("TestPageQuery|IDX|FAIL|%d\n", count)
		return
	}

	startIdx = 0
	hasMore = true
	//开始分页查询未过期的消息实体
	for hasMore {
		more, entities := kiteMysql.PageQueryEntity("6c", hn,
			time.Now().Add(8*time.Minute).Unix(), startIdx, 1)
		if len(entities) <= 0 {
			t.Logf("TestPageQuery|CHECK|NO DATA|%s\n", entities)
			break
		}

		//开始发起重投
		for _, entity := range entities {
			if entity.DeliverCount != 1 || entity.FailGroups[0] != "s-mts-test" {
				t.Fail()
			}
			t.Logf("TestPageQuery|PageQueryEntity|CHECK|%s\n", entity.MessageId)
		}
		startIdx += len(entities)
		hasMore = more
	}

	t.Logf("TestPageQuery|CHECK|%d\n", startIdx)
	if startIdx != 10 {
		t.Fail()
	}

	truncate(kiteMysql)

}

func TestBatch(t *testing.T) {
	options := MysqlOptions{
		Addr:         "localhost:3306",
		DB:           "kite",
		Username:     "root",
		Password:     "",
		ShardNum:     4,
		BatchUpSize:  100,
		BatchDelSize: 100,
		FlushPeriod:  10 * time.Millisecond,
		MaxIdleConn:  10,
		MaxOpenConn:  10}

	kiteMysql := NewKiteMysql(options, "localhost")

	truncate(kiteMysql)

	mids := make([]string, 0, 16)
	for i := 0; i < 16; i++ {
		//创建消息
		msg := &protocol.BytesMessage{}
		msg.Header = &protocol.Header{
			MessageId:    proto.String("26c03f00665862591f696a980b5a6" + fmt.Sprintf("%x", i)),
			Topic:        proto.String("trade"),
			MessageType:  proto.String("pay-succ"),
			ExpiredTime:  proto.Int64(time.Now().Add(10 * time.Minute).Unix()),
			DeliverLimit: proto.Int32(100),
			GroupId:      proto.String("go-kite-test"),
			Commit:       proto.Bool(false),
			Fly:          proto.Bool(false)}
		msg.Body = []byte("hello world")

		entity := store.NewMessageEntity(protocol.NewQMessage(msg))
		entity.SuccGroups = []string{"go-kite-test"}
		hn := "localhost"
		entity.KiteServer = hn
		entity.PublishTime = time.Now().Unix()
		kiteMysql.Save(entity)
		mids = append(mids, entity.MessageId)
	}

	for _, v := range mids {

		msg := &store.MessageEntity{
			MessageId:    v,
			DeliverCount: 1,
			SuccGroups:   []string{"s-mts-test"},
			FailGroups:   []string{},
			//设置一下下一次投递时间
			NextDeliverTime: time.Now().Unix()}
		kiteMysql.AsyncUpdate(msg)
	}

	time.Sleep(5 * time.Second)
	for _, v := range mids {
		e := kiteMysql.Query(v)
		if nil == e || len(e.SuccGroups) < 1 {
			t.Fatalf("TestBatch|Update FAIL|%s|%s\n", e, v)
			t.Fail()
			return
		}
		t.Logf("Query|%s\n", e)
	}

	//测试批量删除
	for _, v := range mids {
		kiteMysql.AsyncDelete(v)
	}
	time.Sleep(5 * time.Second)
	for _, v := range mids {
		entity := kiteMysql.Query(v)
		if nil != entity {
			t.Fatalf("TestBatch|AysncDelete FAIL|%s\n", entity)
			t.Fail()

		}
	}

	truncate(kiteMysql)
}

func truncate(k *KiteMysqlStore) {
	for i := 0; i < 4; i++ {
		for j := 0; j < 4; j++ {
			m := k.dbshard.FindShardById(i*4 + j).master
			_, err := m.Exec(fmt.Sprintf("truncate table kite_msg_%d", j))
			if nil != err {
				log.Printf("ERROR|truncate table kite_msg_%d.%s|%s\n", i, j, err)
			} else {
				// log.Printf("SUCC|truncate table kite_msg_%d.%s|%s\n", i, j, err)
			}
			_, err = m.Exec(fmt.Sprintf("truncate table kite_msg_dlq"))
			if nil != err {
				log.Printf("ERROR|truncate table kite_msg_%d.kite_msg_dlq|%s\n", i, j, err)
			} else {
				// log.Printf("SUCC|truncate table kite_msg_%d.%s|%s\n", i, j, err)
			}
		}
	}
}

func TestStringSave(t *testing.T) {

	options := MysqlOptions{
		Addr:         "localhost:3306",
		DB:           "kite",
		Username:     "root",
		Password:     "",
		ShardNum:     4,
		BatchUpSize:  100,
		BatchDelSize: 100,
		FlushPeriod:  10 * time.Millisecond,
		MaxIdleConn:  10,
		MaxOpenConn:  10}

	kiteMysql := NewKiteMysql(options, "localhost")
	truncate(kiteMysql)
	for i := 0; i < 16; i++ {
		//创建消息
		msg := &protocol.StringMessage{}
		msg.Header = &protocol.Header{
			MessageId:    proto.String("26c03f00665862591f696a980b5a6" + fmt.Sprintf("%x", i)),
			Topic:        proto.String("trade"),
			MessageType:  proto.String("pay-succ"),
			ExpiredTime:  proto.Int64(time.Now().Add(10 * time.Minute).Unix()),
			DeliverLimit: proto.Int32(100),
			GroupId:      proto.String("go-kite-test"),
			Commit:       proto.Bool(false),
			Fly:          proto.Bool(false)}

		msg.Body = proto.String("hello world")
		innerT(kiteMysql, msg, msg.GetHeader().GetMessageId(), t)
	}
	kiteMysql.Stop()
}

func TestBytesSave(t *testing.T) {

	options := MysqlOptions{
		Addr:         "localhost:3306",
		DB:           "kite",
		Username:     "root",
		Password:     "",
		ShardNum:     4,
		BatchUpSize:  100,
		BatchDelSize: 100,
		FlushPeriod:  10 * time.Millisecond,
		MaxIdleConn:  10,
		MaxOpenConn:  10}

	kiteMysql := NewKiteMysql(options, "localhost")
	truncate(kiteMysql)
	for i := 0; i < 16; i++ {
		//创建消息
		msg := &protocol.BytesMessage{}
		msg.Header = &protocol.Header{
			MessageId:    proto.String("26c03f00665862591f696a980b5a6" + fmt.Sprintf("%x", i)),
			Topic:        proto.String("trade"),
			MessageType:  proto.String("pay-succ"),
			ExpiredTime:  proto.Int64(time.Now().Add(10 * time.Minute).Unix()),
			DeliverLimit: proto.Int32(100),
			GroupId:      proto.String("go-kite-test"),
			Commit:       proto.Bool(false),
			Fly:          proto.Bool(false)}

		msg.Body = []byte("hello world")
		innerT(kiteMysql, msg, msg.GetHeader().GetMessageId(), t)
	}

	truncate(kiteMysql)

	kiteMysql.Stop()
}

//增加迁移
func TestExpiredDLQ(t *testing.T) {

	options := MysqlOptions{
		Addr:         "localhost:3306",
		DB:           "kite",
		Username:     "root",
		Password:     "",
		ShardNum:     4,
		BatchUpSize:  100,
		BatchDelSize: 100,
		FlushPeriod:  10 * time.Millisecond,
		MaxIdleConn:  10,
		MaxOpenConn:  10}

	kiteMysql := NewKiteMysql(options, "localhost")
	truncate(kiteMysql)
	messageIds := make([]string, 0, 10)
	for i := 0; i < 256; i++ {
		//创建消息
		msg := &protocol.BytesMessage{}
		msg.Header = &protocol.Header{
			MessageId:    proto.String("26c03f00665862591f696a980b5a6" + fmt.Sprintf("%x", i)),
			Topic:        proto.String("trade"),
			MessageType:  proto.String("pay-succ"),
			ExpiredTime:  proto.Int64(time.Now().Add(10 * time.Second).Unix()),
			DeliverLimit: proto.Int32(100),
			GroupId:      proto.String("go-kite-test"),
			Commit:       proto.Bool(false),
			Fly:          proto.Bool(false)}

		msg.Body = []byte("hello world")
		qm := protocol.NewQMessage(msg)
		entity := store.NewMessageEntity(qm)
		entity.SuccGroups = []string{"go-kite-test"}
		hn := "localhost"
		entity.KiteServer = hn
		entity.PublishTime = time.Now().Unix()
		entity.DeliverCount = 101

		succ := kiteMysql.Save(entity)
		if !succ {
			t.Fail()
		} else {
			// fmt.Printf("SAVE|SUCC|%s\n", entity)
		}
		messageIds = append(messageIds, msg.GetHeader().GetMessageId())
	}

	//开始清理
	kiteMysql.MoveExpired()

	for _, messageId := range messageIds {

		entity := kiteMysql.Query(messageId)
		if nil != entity {
			t.Fail()
			fmt.Println("MoveExpired|FAIL|" + messageId)
		}
	}

	total := 0
	for i := 0; i < 4; i++ {
		db := kiteMysql.dbshard.FindSlave(fmt.Sprintf("%x", i))
		rows, _ := db.Query("select count(*) from kite_msg_dlq")
		if rows.Next() {
			var count int
			rows.Scan(&count)
			t.Logf("TestExpiredDLQ|COUNT|%s|%d\n", fmt.Sprintf("%x", i), total)
			total += count
		}
	}

	if total != 256 {
		t.Fail()
		t.Logf("TestExpiredDLQ|TOTAL NOT EQUAL|%d\n", total)
	}

	truncate(kiteMysql)
	kiteMysql.Stop()
}

func innerT(kiteMysql *KiteMysqlStore, msg interface{}, msgid string, t *testing.T) {

	qm := protocol.NewQMessage(msg)
	entity := store.NewMessageEntity(qm)
	entity.SuccGroups = []string{"go-kite-test"}
	hn := "localhost"
	entity.KiteServer = hn
	entity.PublishTime = time.Now().Unix()

	succ := kiteMysql.Save(entity)
	if !succ {
		t.Fail()
	} else {
		t.Logf("SAVE|SUCC|%s\n", entity)
	}

	ret := kiteMysql.Query(msgid)
	t.Logf("Query|%s|%s\n", msgid, ret)
	if nil == ret {
		t.Fail()
		return
	}

	bb, ok := qm.GetBody().([]byte)
	if ok {
		rb, _ := ret.GetBody().([]byte)
		if string(rb) != string(bb) {
			t.Fail()
		} else {
			t.Logf("Query|SUCC|%s\n", ret)
		}
	} else {
		bs, _ := qm.GetBody().(string)
		rs, _ := ret.GetBody().(string)
		if bs != rs {
			t.Fail()
		} else {
			t.Logf("Query|SUCC|%s\n", ret)
		}
	}

	t.Logf("Commint BEGIN")
	commit := kiteMysql.Commit(msgid)
	if !commit {
		t.Logf("Commint FAIL")
		t.Fail()
	}
	t.Logf("Commint END")
	time.Sleep(200 * time.Millisecond)
	ret = kiteMysql.Query(msgid)
	t.Logf("PageQueryEntity|COMMIT RESULT|%s\n", ret)
	if !ret.Commit {
		t.Logf("Commit|FAIL|%s\n", ret)
		t.Fail()
	}

	hasNext, entities := kiteMysql.PageQueryEntity(msgid, hn, time.Now().Unix(), 0, 10)
	t.Logf("PageQueryEntity|%s\n", entities)
	if hasNext {
		t.Logf("PageQueryEntity|FAIL|HasNext|%s\n", entities)
		t.Fail()
	} else {
		if len(entities) != 1 {
			t.Logf("PageQueryEntity|FAIL|%s\n", entities)
			t.Fail()
		} else {
			if entities[0].Header.GetMessageId() != qm.GetHeader().GetMessageId() {
				t.Fail()
			}
		}
	}
}
