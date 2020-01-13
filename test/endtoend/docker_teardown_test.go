package endtoend

import (
	"context"
	"github.com/stretchr/testify/suite"
	titanclient "github.com/titan-data/titan-client-go"
	"testing"
)

type TeardownTestSuite struct {
	suite.Suite
	docker *dockerUtil
}

func (s *TeardownTestSuite) SetupSuite() {
	s.docker = DockerUtil("docker-zfs")
	err := s.docker.StopServer(true)
	if err != nil {
		panic(err)
	}
	err = s.docker.StartServer()
	if err != nil {
		panic(err)
	}
	err = s.docker.WaitForServer()
	if err != nil {
		panic(err)
	}
}

func (s *TeardownTestSuite) TearDownSuite() {
	err := s.docker.StopServer(false)
	if err != nil {
		panic(err)
	}
}

func (s *TeardownTestSuite) TestTeardownCreateRepository() {
	_, _, err := s.docker.Client.RepositoriesApi.CreateRepository(context.Background(), titanclient.Repository{
		Name:       "foo",
		Properties: map[string]interface{}{"a": "b"},
	})
	if err != nil {
		s.Fail(err.Error())
	}
}

func (s *TeardownTestSuite) TestTeardownGetRepository() {
	repo, _, _ := s.docker.Client.RepositoriesApi.GetRepository(context.Background(), "foo")
	s.NotNil(repo)
	s.Equal("foo", repo.Name)
	s.Len(repo.Properties, 1)
	s.Equal("b", repo.Properties["a"])
}

func (s *TeardownTestSuite) TestTeardownRestart() {
	err := s.docker.RestartServer()
	if err != nil {
		s.FailNow(err.Error())
	}
	err = s.docker.WaitForServer()
	if err != nil {
		s.FailNow(err.Error())
	}
	repo, _, _ := s.docker.Client.RepositoriesApi.GetRepository(context.Background(), "foo")
	s.NotNil(repo)
	s.Equal("foo", repo.Name)
}

func (s *TeardownTestSuite) TestTeardownRestartTeardown() {
	err := s.docker.StopServer(false)
	if err != nil {
		s.FailNow(err.Error())
	}
	err = s.docker.StartServer()
	if err != nil {
		s.FailNow(err.Error())
	}
	err = s.docker.WaitForServer()
	if err != nil {
		s.FailNow(err.Error())
	}
	_, _, err = s.docker.Client.RepositoriesApi.GetRepository(context.Background(), "foo")
	_ = s.Error(err)
}

func TestTeardownTestSuite(t *testing.T) {
	suite.Run(t, new(TeardownTestSuite))
}
