/*
 * Copyright The Titan Project Contributors.
 */
package kubernetes

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	titan "github.com/titan-data/titan-client-go"
	endtoend "github.com/titan-data/titan-server/test/common"
	coreV1 "k8s.io/api/core/v1"
	apiV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

type KubernetesWorkflowTestSuite struct {
	suite.Suite
	e *endtoend.EndToEndTest
	*kubernetes.Clientset
	namespace    string
	ctx          context.Context
	uuid         string
	pod1         string
	pod2         string
	remote titan.Remote
	remoteParams titan.RemoteParameters
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

	uuid, err := uuid.NewRandom()
	if err != nil {
		panic(err)
	}
	s.uuid = uuid.String()
	s.pod1 = fmt.Sprintf("%s-test", s.uuid)
	s.pod2 = fmt.Sprintf("%s-test2", s.uuid)
	s.Clientset = cs
	s.namespace = "default"
	s.ctx = context.Background()
	s.remote = titan.Remote{
		Provider:   "nop",
		Name:       "origin",
		Properties: map[string]interface{}{},
	}
	s.remoteParams = titan.RemoteParameters{
		Provider:   "nop",
		Properties: map[string]interface{}{},
	}
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

func (s *KubernetesWorkflowTestSuite) WaitForPod(name string) error {
	ready := false
	for ok := true; ok; ok = !ready {
		res, err := s.Clientset.CoreV1().Pods(s.namespace).Get(name, apiV1.GetOptions{})
		if err != nil {
			return err
		}

		if len(res.Status.ContainerStatuses) != 0 {
			ready = true
		}
		for _, container := range res.Status.ContainerStatuses {
			if container.RestartCount != 0 {
				return fmt.Errorf("container %s restarted %d times", name, container.RestartCount)
			}
			if !container.Ready {
				ready = false
			}
		}

		if !ready {
			time.Sleep(time.Duration(1) * time.Second)
		}
	}
	return nil
}

func (s *KubernetesWorkflowTestSuite) LaunchPod(name string, claim string) error {
	_, err := s.Clientset.CoreV1().Pods(s.namespace).Create(&coreV1.Pod{
		ObjectMeta: apiV1.ObjectMeta{
			Name: name,
		},
		Spec: coreV1.PodSpec{
			Containers: []coreV1.Container{{
				Name:    "test",
				Image:   "ubuntu:bionic",
				Command: []string{"/bin/sh"},
				Args:    []string{"-c", "while true; do sleep 5; done"},
				VolumeMounts: []coreV1.VolumeMount{{
					Name:      "data",
					MountPath: "/data",
				}},
			}},
			Volumes: []coreV1.Volume{{
				Name: "data",
				VolumeSource: coreV1.VolumeSource{
					PersistentVolumeClaim: &coreV1.PersistentVolumeClaimVolumeSource{
						ClaimName: claim,
					},
				},
			}},
		},
	})
	return err
}

func (s *KubernetesWorkflowTestSuite) TestKubernetesConfig_001_GetContext() {
	res, _, err := s.e.Client.ContextsApi.GetContext(context.Background())
	if s.e.NoError(err) {
		s.Equal("kubernetes-csi", res.Provider)
	}
}

func (s *KubernetesWorkflowTestSuite) TestKubernetesConfig_002_Kubectl() {
	_, err := s.e.ExecServer("kubectl", "cluster-info")
	s.e.NoError(err)
}

func (s *KubernetesWorkflowTestSuite) TestKubernetesConfig_010_CreateRepository() {
	_, _, err := s.e.RepoApi.CreateRepository(s.ctx, titan.Repository{
		Name:       "foo",
		Properties: map[string]interface{}{},
	})
	s.e.NoError(err)
}

func (s *KubernetesWorkflowTestSuite) TestKubernetesConfig_011_CreateVolume() {
	_, _, err := s.e.VolumeApi.CreateVolume(s.ctx, "foo", titan.Volume{
		Name:       "vol",
		Properties: map[string]interface{}{},
	})
	s.e.NoError(err)
}

func (s *KubernetesWorkflowTestSuite) TestKubernetesConfig_012_LaunchPod() {
	vol, _, err := s.e.VolumeApi.GetVolume(s.ctx, "foo", "vol")
	if s.e.NoError(err) {
		pvc := vol.Config["pvc"].(string)
		err = s.LaunchPod(s.pod1, pvc)
		if s.e.NoError(err) {
			err = s.WaitForPod(s.pod1)
			s.e.NoError(err)
		}
	}
}

func (s *KubernetesWorkflowTestSuite) TestKubernetesConfig_013_WriteData() {
	err := exec.Command("kubectl", "exec", s.pod1, "--", "sh", "-c", "echo one > /data/out; sync; sleep 1;").Run()
	s.e.NoError(err)
}

func (s *KubernetesWorkflowTestSuite) TestKubernetesConfig_014_VolumeStatus() {
	err := s.e.WaitForVolume("foo", "vol")
	if s.e.NoError(err) {
		res, _, err := s.e.VolumeApi.GetVolumeStatus(s.ctx, "foo", "vol")
		if s.e.NoError(err) {
			s.True(res.Ready)
			s.Empty(res.Error)
		}
	}
}

func (s *KubernetesWorkflowTestSuite) TestKubernetesConfig_020_CreateCommit() {
	_, _, err := s.e.CommitApi.CreateCommit(s.ctx, "foo", titan.Commit{
		Id:         "id",
		Properties: map[string]interface{}{},
	})
	s.e.NoError(err)
}

func (s *KubernetesWorkflowTestSuite) TestKubernetesConfig_021_LaunchNewPod() {
	vol, _, err := s.e.VolumeApi.GetVolume(s.ctx, "foo", "vol")
	if s.e.NoError(err) {
		pvc := vol.Config["pvc"].(string)
		err = s.LaunchPod(s.pod2, pvc)
		if s.e.NoError(err) {
			err = s.WaitForPod(s.pod2)
			s.e.NoError(err)
		}
	}
}


func (s *KubernetesWorkflowTestSuite) TestKubernetesConfig_021_CommitStatus() {
	err := s.e.WaitForCommit("foo", "id")
	if s.e.NoError(err) {
		res, _, err := s.e.CommitApi.GetCommitStatus(s.ctx, "foo", "id")
		if s.e.NoError(err) {
			s.True(res.Ready)
			s.Empty(res.Error)
		}
	}
}

func (s *KubernetesWorkflowTestSuite) TestKubernetesConfig_022_UpdateData() {
	err := exec.Command("kubectl", "exec", s.pod1, "--", "sh", "-c", "echo two > /data/out; sync; sleep 1;").Run()
	s.e.NoError(err)
}

func (s *KubernetesWorkflowTestSuite) TestKubernetesConfig_023_DeletePod() {
	err := exec.Command("kubectl", "delete", "pod", "--grace-period=0", "--force", s.pod1).Run()
	s.e.NoError(err)
}

func (s *KubernetesWorkflowTestSuite) TestKubernetesConfig_030_Checkout() {
	_, err := s.e.CommitApi.CheckoutCommit(s.ctx, "foo", "id")
	s.e.NoError(err)
}

func (s *KubernetesWorkflowTestSuite) TestKubernetesConfig_031_VerifyContents() {
	out, err := exec.Command("kubectl", "exec", s.pod2, "cat", "/data/out").Output()
	if s.e.NoError(err) {
		s.Equal("one", strings.TrimSpace(string(out)))
	}
}

func (s *KubernetesWorkflowTestSuite) TestKubernetesConfig_032_DeleteClonedPod() {
	err := exec.Command("kubectl", "delete", "pod", "--grace-period=0", "--force", s.pod2).Run()
	s.e.NoError(err)
}

func (s *KubernetesWorkflowTestSuite) TestKubernetesConfig_040_AddRemote() {
	_, _, err := s.e.RemoteApi.CreateRemote(s.ctx, "foo", s.remote)
	s.e.NoError(err)
}

func (s *KubernetesWorkflowTestSuite) TestKubernetesConfig_041_Push() {
	op, _, err := s.e.OperationsApi.Push(s.ctx, "foo", "origin", "id", s.remoteParams, nil)
	if s.e.NoError(err) {
		_, err = s.e.WaitForOperation(op.Id)
		s.e.NoError(err)
	}
}

func (s *KubernetesWorkflowTestSuite) TestKubernetesConfig_042_DeleteCommit() {
	_, err := s.e.CommitApi.DeleteCommit(s.ctx, "foo", "id")
	s.e.NoError(err)
}

func (s *KubernetesWorkflowTestSuite) TestKubernetesConfig_043_Pull() {
	op, _, err := s.e.OperationsApi.Pull(s.ctx, "foo", "origin", "id", s.remoteParams, nil)
	if s.e.NoError(err) {
		_, err = s.e.WaitForOperation(op.Id)
		s.e.NoError(err)
	}
}

func (s *KubernetesWorkflowTestSuite) TestKubernetesConfig_070_DeleteVolume() {
	_, err := s.e.VolumeApi.DeactivateVolume(s.ctx, "foo", "vol")
	if s.e.NoError(err) {
		_, err = s.e.VolumeApi.DeleteVolume(s.ctx, "foo", "vol")
		s.e.NoError(err)
	}
}

func (s *KubernetesWorkflowTestSuite) TestKubernetesConfig_071_DeleteRepository() {
	_, err := s.e.RepoApi.DeleteRepository(s.ctx, "foo")
	s.e.NoError(err)
}
