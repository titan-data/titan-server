package docker

import (
	"context"
	"github.com/stretchr/testify/suite"
	titanclient "github.com/titan-data/titan-client-go"
	endtoend "github.com/titan-data/titan-server/test/common"
	"testing"
)

type TeardownTestSuite struct {
	suite.Suite
	e *endtoend.EndToEndTest
}

func (s *TeardownTestSuite) SetupSuite() {
	s.e = endtoend.NewEndToEndTest(&s.Suite, "docker-zfs")
	s.e.SetupStandardDocker()
}

func (s *TeardownTestSuite) TearDownSuite() {
	s.e.TeardownStandardDocker()
}

func (s *TeardownTestSuite) TestTeardown_001_CreateRepository() {
	_, _, err := s.e.Client.RepositoriesApi.CreateRepository(context.Background(), titanclient.Repository{
		Name:       "foo",
		Properties: map[string]interface{}{"a": "b"},
	})
	s.e.NoError(err)
}

func (s *TeardownTestSuite) TestTeardown_002_GetRepository() {
	repo, _, err := s.e.Client.RepositoriesApi.GetRepository(context.Background(), "foo")
	if s.e.NoError(err) {
		s.Equal("foo", repo.Name)
		s.Len(repo.Properties, 1)
		s.Equal("b", repo.Properties["a"])
	}
}

func (s *TeardownTestSuite) TestTeardown_003_Restart() {
	err := s.e.RestartServer()
	s.e.NoError(err)
	err = s.e.WaitForServer()
	s.e.NoError(err)
	repo, _, err := s.e.Client.RepositoriesApi.GetRepository(context.Background(), "foo")
	if s.e.NoError(err) {
		s.NotNil(repo)
		s.Equal("foo", repo.Name)
	}
}

func (s *TeardownTestSuite) TestTeardown_004_RestartTeardown() {
	err := s.e.StopServer(false)
	s.e.NoError(err)
	err = s.e.StartServer()
	s.e.NoError(err)
	err = s.e.WaitForServer()
	s.e.NoError(err)
	_, _, err = s.e.Client.RepositoriesApi.GetRepository(context.Background(), "foo")
	s.e.APIError(err, "NoSuchObjectException")
}

func TestTeardownTestSuite(t *testing.T) {
	suite.Run(t, new(TeardownTestSuite))
}
