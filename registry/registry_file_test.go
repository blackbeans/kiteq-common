package registry

import (
	"context"
	"github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestFileRegistry(t *testing.T) {
	convey.Convey("TestFileRegistry", t, func() {
		fr := NewFileRegistry(context.TODO(), "registry_demo.yaml")
		fr.Start()

		group2Binds, err := fr.GetBindAndWatch("user-profile")
		convey.So(err, convey.ShouldBeNil)
		t.Logf("%v\n", group2Binds)
		_, ok := group2Binds["s-user-profile"]
		convey.So(ok, convey.ShouldBeTrue)

		brokers, err := fr.GetQServerAndWatch("user-profile")
		convey.So(err, convey.ShouldBeNil)
		t.Logf("%s\n", brokers)
		convey.So(len(brokers), convey.ShouldEqual, 2)

	})
}
