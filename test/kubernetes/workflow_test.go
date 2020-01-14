/*
 * Copyright The Titan Project Contributors.
 */
package kubernetes

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/suite"
	endtoend "github.com/titan-data/titan-server/test/common"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"os"
	"strings"
	"testing"
)

type KubernetesWorkflowTestSuite struct {
	suite.Suite
	e *endtoend.EndToEndTest
	*kubernetes.Clientset
}

func (s *KubernetesWorkflowTestSuite) SetupSuite() {
	s.e = endtoend.NewEndToEndTest(&s.Suite, "kubernetes-csi")
	_ = s.e.StopServer(true)

	config := strings.Split(os.Getenv("KUBERNETES_CONFIG"), ",")
	_ = s.e.StopServer(true)
	err := s.e.StartServer(config...)
	if err != nil {
		panic(err)
	}
	err = s.e.WaitForServer()
	if err != nil {
		panic(err)
	}

	cfg, err := clientcmd.BuildConfigFromFlags("", fmt.Sprintf("%s/.kube/config", s.e.HomeDir))
	if err != nil {
		panic(err)
	}

	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		panic(err)
	}

	s.Clientset = cs
}

func (s *KubernetesWorkflowTestSuite) TearDownSuite() {
	err := s.e.StopServer(false)
	if err != nil {
		panic(err)
	}
}

func TestKubernetesWorkflowTestSuite(t *testing.T) {
	suite.Run(t, new(KubernetesWorkflowTestSuite))
}

func (s *KubernetesWorkflowTestSuite) WaitForPod(name string) {
}

func (s *KubernetesWorkflowTestSuite) TestKubernetesConfig_001_GetContext() {
	res, _, err := s.e.Client.ContextsApi.GetContext(context.Background())
	if s.e.NoError(err) {
		s.Equal("kubernetes-csi", res.Provider)
	}
}
