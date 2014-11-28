package goshine

import (
	"fmt"
	"testing"
)

func Test_Connect(t *testing.T) {
	s := NewGoshine("172.16.34.125", 10000, "user", "passwd", "douban")

	if s.Connect() != nil {
		t.Error("init goshine instance error")
	}
}

func Test_Execute(t *testing.T) {
	s := NewGoshine("172.16.34.125", 10000, "user", "passwd", "douban")

	if s.Connect() != nil {
		t.Error("init goshine instance error")
	}

	if err := s.Execute("use douban"); err != nil {
		t.Error("execute hql error", err)
	}

	s.Close()
}

func Test_FetchAll(t *testing.T) {
	s := NewGoshine("172.16.34.125", 10000, "user", "passwd", "douban")

	if s.Connect() != nil {
		t.Error("init goshine instance error")
	}

	if err := s.Execute("use douban"); err != nil {
		t.Error("execute hql error", err)
	}

	for i := 0; i < 100; i++ {
		ret, err := s.FetchAll("select *, 1/3 from contact limit 1")
		if err != nil {
			t.Error("execute hql error", err)
		}
		fmt.Println(ret)
	}

	s.Close()
}

func Test_NoConnection(t *testing.T) {
	s := NewGoshine("172.16.34.125", 10000, "user", "passwd", "douban")

	if err := s.Execute("use douban"); err == nil {
		t.Error("should return an error")
	}
}
