/*
 * Copyright The Titan Project Contributors.
 */
package kubernetes

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/suite"
	"os/exec"
	"strings"
	"testing"
	endtoend "github.com/titan-data/titan-server/test/common"
)

type KubernetesConfigTestSuite struct {
	suite.Suite
	e *endtoend.EndToEndTest

	ConfigFile    string
	KubeContext   string
	StorageClass  string
	SnapshotClass string
}

func (s *KubernetesConfigTestSuite) SetupSuite() {
	s.e = endtoend.NewEndToEndTest(&s.Suite, "kubernetes-csi")
	_ = s.e.StopServer(true)

	s.ConfigFile = "config"
	out, err := exec.Command("kubectl", "config", "current-context").Output()
	if err != nil {
		panic(err)
	}
	s.KubeContext = strings.TrimSpace(string(out))
	s.StorageClass = "noSuchClass"
	s.SnapshotClass = "noSuchClass"
}

func (s *KubernetesConfigTestSuite) TearDownSuite() {
	err := s.e.StopServer(false)
	if err != nil {
		panic(err)
	}
}

func TestKubernetesConfigSuite(t *testing.T) {
	suite.Run(t, new(KubernetesConfigTestSuite))
}

func (s *KubernetesConfigTestSuite) TestKubernetesConfig_001_StartServer() {
	err := s.e.StartServer(
		fmt.Sprintf("configFile=%s", s.ConfigFile),
		fmt.Sprintf("context=%s", s.KubeContext),
		fmt.Sprintf("storageClass=%s", s.StorageClass),
		fmt.Sprintf("snapshotClass=%s", s.SnapshotClass))
	if s.e.NoError(err) {
		err := s.e.WaitForServer()
		s.e.NoError(err)
	}
}

func (s *KubernetesConfigTestSuite) TestKubernetesConfig_002_GetConfiguration() {
	res, _, err := s.e.Client.ContextsApi.GetContext(context.Background())
	if s.e.NoError(err) {
		s.Len(res.Properties, 4)
		s.Equal(s.ConfigFile, res.Properties["configFile"])
		s.Equal(s.KubeContext, res.Properties["context"])
		s.Equal(s.StorageClass, res.Properties["storageClass"])
		s.Equal(s.SnapshotClass, res.Properties["snapshotClass"])
	}
}
