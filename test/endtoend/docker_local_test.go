/*
 * Copyright The Titan Project Contributors
 */
package endtoend

import (
	"context"
	"github.com/antihax/optional"
	"github.com/stretchr/testify/suite"
	titan "github.com/titan-data/titan-client-go"
	"strings"
	"testing"
	"time"
)

type DockerLocalTestSuite struct {
	suite.Suite
	e   *EndToEndTest
	ctx context.Context

	volumeMountpoint string
	remoteParams     titan.RemoteParameters
	currentOp        titan.Operation
}

func (s *DockerLocalTestSuite) SetupSuite() {
	s.e = NewEndToEndTest(&s.Suite, "docker-zfs")
	s.e.SetupStandardDocker()
	s.ctx = context.Background()

	s.remoteParams = titan.RemoteParameters{
		Provider:   "nop",
		Properties: map[string]interface{}{},
	}
}

func (s *DockerLocalTestSuite) TearDownSuite() {
	s.e.TeardownStandardDocker()
}

func TestDockerLocalSuite(t *testing.T) {
	suite.Run(t, new(DockerLocalTestSuite))
}

func (s *DockerLocalTestSuite) TestLocal_001_GetContext() {
	res, _, err := s.e.Client.ContextsApi.GetContext(s.ctx)
	if s.e.NoError(err) {
		s.Equal("docker-zfs", res.Provider)
		s.Len(res.Properties, 1)
		s.Equal("test", res.Properties["pool"])
	}
}

func (s *DockerLocalTestSuite) TestLocal_002_EmptyRepoList() {
	res, _, err := s.e.RepoApi.ListRepositories(s.ctx)
	if s.e.NoError(err) {
		s.Len(res, 0)
	}
}

func (s *DockerLocalTestSuite) TestLocal_003_CreateRepository() {
	res, _, err := s.e.RepoApi.CreateRepository(s.ctx, titan.Repository{
		Name:       "foo",
		Properties: map[string]interface{}{"a": "b"},
	})
	if s.e.NoError(err) {
		s.Equal("foo", res.Name)
		s.Len(res.Properties, 1)
		s.Equal("b", res.Properties["a"])
	}
}

func (s *DockerLocalTestSuite) TestLocal_004_GetRepository() {
	res, _, err := s.e.RepoApi.GetRepository(s.ctx, "foo")
	if s.e.NoError(err) {
		s.Equal("foo", res.Name)
		s.Len(res.Properties, 1)
		s.Equal("b", res.Properties["a"])
	}
}

func (s *DockerLocalTestSuite) TestLocal_005_ListRepositoryPresent() {
	res, _, err := s.e.RepoApi.ListRepositories(s.ctx)
	if s.e.NoError(err) {
		s.Len(res, 1)
		repo := res[0]
		s.Equal("foo", repo.Name)
		s.Len(repo.Properties, 1)
		s.Equal("b", repo.Properties["a"])
	}
}

func (s *DockerLocalTestSuite) TestLocal_006_CreateDuplicate() {
	_, _, err := s.e.RepoApi.CreateRepository(s.ctx, titan.Repository{
		Name:       "foo",
		Properties: map[string]interface{}{},
	})
	s.e.APIError(err, "ObjectExistsException")
}

func (s *DockerLocalTestSuite) TestLocal_010_CreateVolume() {
	res, _, err := s.e.VolumeApi.CreateVolume(s.ctx, "foo", titan.Volume{
		Name:       "vol",
		Properties: map[string]interface{}{"a": "b"},
	})
	if s.e.NoError(err) {
		s.Equal("vol", res.Name)
		s.Len(res.Properties, 1)
		s.Equal("b", res.Properties["a"])
	}
}

func (s *DockerLocalTestSuite) TestLocal_011_CreateVolumeBadRepo() {
	_, _, err := s.e.VolumeApi.CreateVolume(s.ctx, "bar", titan.Volume{
		Name:       "vol",
		Properties: map[string]interface{}{"a": "b"},
	})
	s.e.APIError(err, "NoSuchObjectException")
}

func (s *DockerLocalTestSuite) TestLocal_012_CreateVolumeDuplicate() {
	_, _, err := s.e.VolumeApi.CreateVolume(s.ctx, "foo", titan.Volume{
		Name:       "vol",
		Properties: map[string]interface{}{"a": "b"},
	})
	s.e.APIError(err, "ObjectExistsException")
}

func (s *DockerLocalTestSuite) TestLocal_013_GetVolume() {
	res, _, err := s.e.VolumeApi.GetVolume(s.ctx, "foo", "vol")
	if s.e.NoError(err) {
		s.Equal("vol", res.Name)
		s.Len(res.Properties, 1)
		s.Equal("b", res.Properties["a"])
		s.volumeMountpoint = res.Config["mountpoint"].(string)
		idx := strings.Index(s.volumeMountpoint, "/var/lib/test/mnt/")
		s.Equal(0, idx)
	}
}

func (s *DockerLocalTestSuite) TestLocal_014_GetBadVolume() {
	_, _, err := s.e.VolumeApi.GetVolume(s.ctx, "bar", "vol")
	s.e.APIError(err, "NoSuchObjectException")
}

func (s *DockerLocalTestSuite) TestLocal_015_ListVolume() {
	res, _, err := s.e.VolumeApi.ListVolumes(s.ctx, "foo")
	if s.e.NoError(err) {
		s.Len(res, 1)
		s.Equal("vol", res[0].Name)
	}
}

func (s *DockerLocalTestSuite) TestLocal_016_MountVolume() {
	_, err := s.e.VolumeApi.ActivateVolume(s.ctx, "foo", "vol")
	s.e.NoError(err)
}

func (s *DockerLocalTestSuite) TestLocal_017_CreateFile() {
	err := s.e.WriteFile("foo", "vol", "testfile", "Hello")
	if s.e.NoError(err) {
		res, err := s.e.ReadFile("foo", "vol", "testfile")
		if s.e.NoError(err) {
			s.Equal("Hello", res)
		}
	}
}

func (s *DockerLocalTestSuite) TestLocal_020_LastCommitEmpty() {
	res, _, err := s.e.RepoApi.GetRepositoryStatus(s.ctx, "foo")
	if s.e.NoError(err) {
		s.Empty(res.SourceCommit)
		s.Empty(res.LastCommit)
	}
}

func (s *DockerLocalTestSuite) TestLocal_021_CreateCommit() {
	res, _, err := s.e.CommitApi.CreateCommit(s.ctx, "foo", titan.Commit{
		Id: "id",
		Properties: map[string]interface{}{"tags": map[string]string{
			"a": "b",
			"c": "e",
		}},
	})
	if s.e.NoError(err) {
		s.Equal("id", res.Id)
		s.Equal("b", s.e.GetTag(res, "a"))
	}
}

func (s *DockerLocalTestSuite) TestLocal_022_DuplicateCommit() {
	_, _, err := s.e.CommitApi.CreateCommit(s.ctx, "foo", titan.Commit{
		Id:         "id",
		Properties: map[string]interface{}{},
	})
	s.e.APIError(err, "ObjectExistsException")
}

func (s *DockerLocalTestSuite) TestLocal_023_GetCommit() {
	res, _, err := s.e.CommitApi.GetCommit(s.ctx, "foo", "id")
	if s.e.NoError(err) {
		s.Equal("id", res.Id)
		s.Equal("b", s.e.GetTag(res, "a"))
	}
}

func (s *DockerLocalTestSuite) TestLocal_024_GetBadCommit() {
	_, _, err := s.e.CommitApi.GetCommit(s.ctx, "foo", "id2")
	s.e.APIError(err, "NoSuchObjectException")
}

func (s *DockerLocalTestSuite) TestLocal_025_UpdateCommit() {
	res, _, err := s.e.CommitApi.UpdateCommit(s.ctx, "foo", "id", titan.Commit{
		Id: "id",
		Properties: map[string]interface{}{"tags": map[string]string{
			"a": "B",
			"c": "e",
		}},
	})
	if s.e.NoError(err) {
		s.Equal("id", res.Id)
		s.Equal("B", s.e.GetTag(res, "a"))
		res, _, _ = s.e.CommitApi.GetCommit(s.ctx, "foo", "id")
		s.Equal("B", s.e.GetTag(res, "a"))
	}
}

func (s *DockerLocalTestSuite) TestLocal_026_CommitStatus() {
	res, _, err := s.e.CommitApi.GetCommitStatus(s.ctx, "foo", "id")
	if s.e.NoError(err) {
		s.NotZero(res.LogicalSize)
		s.NotZero(res.ActualSize)
	}
}

func (s *DockerLocalTestSuite) TestLocal_027_DeleteBadCommit() {
	_, err := s.e.CommitApi.DeleteCommit(s.ctx, "foo", "id2")
	s.e.APIError(err, "NoSuchObjectException")
}

func (s *DockerLocalTestSuite) TestLocal_030_ListCommit() {
	res, _, err := s.e.CommitApi.ListCommits(s.ctx, "foo", nil)
	if s.e.NoError(err) {
		s.Len(res, 1)
		s.Equal("id", res[0].Id)
	}
}

func (s *DockerLocalTestSuite) TestLocal_031_FilterOut() {
	res, _, err := s.e.CommitApi.ListCommits(s.ctx, "foo", &titan.ListCommitsOpts{
		Tag: optional.NewInterface([]string{"a=c"}),
	})
	if s.e.NoError(err) {
		s.Len(res, 0)
	}
}

func (s *DockerLocalTestSuite) TestLocal_032_FilterPresent() {
	res, _, err := s.e.CommitApi.ListCommits(s.ctx, "foo", &titan.ListCommitsOpts{
		Tag: optional.NewInterface([]string{"a=B"}),
	})
	if s.e.NoError(err) {
		s.Len(res, 1)
		s.Equal("id", res[0].Id)
	}
}

func (s *DockerLocalTestSuite) TestLocal_033_FilterCompound() {
	res, _, err := s.e.CommitApi.ListCommits(s.ctx, "foo", &titan.ListCommitsOpts{
		Tag: optional.NewInterface([]string{"a=B", "c"}),
	})
	s.Len(res, 1)
	if s.e.NoError(err) {
		s.Equal("id", res[0].Id)
	}
}

func (s *DockerLocalTestSuite) TestLocal_034_RepositoryStatus() {
	res, _, err := s.e.RepoApi.GetRepositoryStatus(s.ctx, "foo")
	if s.e.NoError(err) {
		s.Equal("id", res.SourceCommit)
		s.Equal("id", res.LastCommit)
	}
}

func (s *DockerLocalTestSuite) TestLocal_040_VolumeStatus() {
	res, _, err := s.e.VolumeApi.GetVolumeStatus(s.ctx, "foo", "vol")
	if s.e.NoError(err) {
		s.Equal("vol", res.Name)
		s.NotZero(res.ActualSize)
		s.NotZero(res.LogicalSize)
		s.True(res.Ready)
		s.Empty(res.Error)
	}
}

func (s *DockerLocalTestSuite) TestLocal_041_WriteNewValue() {
	err := s.e.WriteFile("foo", "vol", "testfile", "Goodbye")
	if s.e.NoError(err) {
		res, err := s.e.ReadFile("foo", "vol", "testfile")
		if s.e.NoError(err) {
			s.Equal("Goodbye", res)
		}
	}
}

func (s *DockerLocalTestSuite) TestLocal_042_Unmount() {
	_, err := s.e.VolumeApi.DeactivateVolume(s.ctx, "foo", "vol")
	s.e.NoError(err)
}

func (s *DockerLocalTestSuite) TestLocal_043_UnmountIdempotent() {
	_, err := s.e.VolumeApi.DeactivateVolume(s.ctx, "foo", "vol")
	s.e.NoError(err)
}

func (s *DockerLocalTestSuite) TestLocal_044_Checkout() {
	_, err := s.e.CommitApi.CheckoutCommit(s.ctx, "foo", "id")
	if s.e.NoError(err) {
		_, err = s.e.VolumeApi.ActivateVolume(s.ctx, "foo", "vol")
		if s.e.NoError(err) {
			res, err := s.e.ReadFile("foo", "vol", "testfile")
			if s.e.NoError(err) {
				s.Equal("Hello", res)
			}
		}
	}
}

func (s *DockerLocalTestSuite) TestLocal_045_NewMountpoint() {
	res, _, err := s.e.VolumeApi.GetVolume(s.ctx, "foo", "vol")
	if s.e.NoError(err) {
		s.NotEqual(s.volumeMountpoint, res.Config["mountpoint"])
		s.volumeMountpoint = res.Config["mountpoint"].(string)
	}
}

func (s *DockerLocalTestSuite) TestLocal_046_SourceCommit() {
	res, _, err := s.e.RepoApi.GetRepositoryStatus(s.ctx, "foo")
	if s.e.NoError(err) {
		s.Equal("id", res.SourceCommit)
		s.Equal("id", res.LastCommit)
	}
}

func (s *DockerLocalTestSuite) TestLocal_050_AddRemote() {
	res, _, err := s.e.RemoteApi.CreateRemote(s.ctx, "foo", titan.Remote{
		Provider:   "nop",
		Name:       "a",
		Properties: map[string]interface{}{},
	})
	if s.e.NoError(err) {
		s.Equal("a", res.Name)
	}
}

func (s *DockerLocalTestSuite) TestLocal_051_GetRemote() {
	res, _, err := s.e.RemoteApi.GetRemote(s.ctx, "foo", "a")
	if s.e.NoError(err) {
		s.Equal("nop", res.Provider)
		s.Equal("a", res.Name)
	}
}

func (s *DockerLocalTestSuite) TestLocal_052_DuplicateRemote() {
	_, _, err := s.e.RemoteApi.CreateRemote(s.ctx, "foo", titan.Remote{
		Provider:   "nop",
		Name:       "a",
		Properties: map[string]interface{}{},
	})
	s.e.APIError(err, "ObjectExistsException")
}

func (s *DockerLocalTestSuite) TestLocal_053_ListRemotes() {
	res, _, err := s.e.RemoteApi.ListRemotes(s.ctx, "foo")
	if s.e.NoError(err) {
		s.Len(res, 1)
		s.Equal("a", res[0].Name)
	}
}

func (s *DockerLocalTestSuite) TestLocal_054_ListRemoteCommits() {
	res, _, err := s.e.RemoteApi.ListRemoteCommits(s.ctx, "foo", "a", s.remoteParams, nil)
	if s.e.NoError(err) {
		s.Len(res, 0)
	}
}

func (s *DockerLocalTestSuite) TestLocal_055_GetRemoteCommit() {
	res, _, err := s.e.RemoteApi.GetRemoteCommit(s.ctx, "foo", "a", "hash", s.remoteParams)
	if s.e.NoError(err) {
		s.Equal("hash", res.Id)
	}
}

func (s *DockerLocalTestSuite) TestLocal_056_DeleteNonExistentRemote() {
	_, err := s.e.RemoteApi.DeleteRemote(s.ctx, "foo", "b")
	s.e.APIError(err, "NoSuchObjectException")
}

func (s *DockerLocalTestSuite) TestLocal_057_UpdateRemote() {
	_, _, err := s.e.RemoteApi.UpdateRemote(s.ctx, "foo", "a", titan.Remote{
		Provider:   "nop",
		Name:       "b",
		Properties: map[string]interface{}{},
	})
	if s.e.NoError(err) {
		res, _, err := s.e.RemoteApi.GetRemote(s.ctx, "foo", "b")
		if s.e.NoError(err) {
			s.Equal("nop", res.Provider)
			s.Equal("b", res.Name)
		}
	}
}

func (s *DockerLocalTestSuite) TestLocal_060_ListEmptyOperations() {
	res, _, err := s.e.OperationsApi.ListOperations(s.ctx, nil)
	if s.e.NoError(err) {
		s.Len(res, 0)
	}
}

func (s *DockerLocalTestSuite) TestLocal_061_StartPush() {
	res, _, err := s.e.OperationsApi.Push(s.ctx, "foo", "b", "id", s.remoteParams, nil)
	if s.e.NoError(err) {
		s.Equal("id", res.CommitId)
		s.Equal("PUSH", res.Type)
		s.Equal("b", res.Remote)
		s.currentOp = res
	}
}

func (s *DockerLocalTestSuite) TestLocal_062_GetOperation() {
	res, _, err := s.e.OperationsApi.GetOperation(s.ctx, s.currentOp.Id)
	if s.e.NoError(err) {
		s.Equal("id", res.CommitId)
		s.Equal("PUSH", res.Type)
		s.Equal("b", res.Remote)
		s.Equal(s.currentOp.Id, res.Id)
	}
}

func (s *DockerLocalTestSuite) TestLocal_063_ListOperations() {
	res, _, err := s.e.OperationsApi.ListOperations(s.ctx, &titan.ListOperationsOpts{Repository: optional.NewString("foo")})
	if s.e.NoError(err) {
		s.Len(res, 1)
		s.Equal(s.currentOp.Id, res[0].Id)
	}
}

func (s *DockerLocalTestSuite) TestLocal_064_GetPushProgress() {
	time.Sleep(time.Duration(1) * time.Second)
	res, _, err := s.e.OperationsApi.GetOperation(s.ctx, s.currentOp.Id)
	if s.e.NoError(err) {
		s.Equal(res.State, "COMPLETE")
		progress, _, err := s.e.OperationsApi.GetOperationProgress(s.ctx, s.currentOp.Id, nil)
		if s.e.NoError(err) {
			s.Len(progress, 2)
			s.Equal("MESSAGE", progress[0].Type)
			s.Equal("Pushing id to 'b'", progress[0].Message)
			s.Equal("COMPLETE", progress[1].Type)
		}
	}
}

func (s *DockerLocalTestSuite) TestLocal_065_ListNotPresent() {
	res, _, err := s.e.OperationsApi.ListOperations(s.ctx, &titan.ListOperationsOpts{Repository: optional.NewString("foo")})
	if s.e.NoError(err) {
		s.Len(res, 0)
	}
}

func (s *DockerLocalTestSuite) TestLocal_070_StartPull() {
	res, _, err := s.e.OperationsApi.Pull(s.ctx, "foo", "b", "id2", s.remoteParams, nil)
	if s.e.NoError(err) {
		s.Equal("id2", res.CommitId)
		s.Equal("PULL", res.Type)
		s.Equal("b", res.Remote)
		s.currentOp = res
	}
}

func (s *DockerLocalTestSuite) TestLocal_071_GetPull() {
	res, _, err := s.e.OperationsApi.GetOperation(s.ctx, s.currentOp.Id)
	if s.e.NoError(err) {
		s.Equal("id2", res.CommitId)
		s.Equal("PULL", res.Type)
		s.Equal("b", res.Remote)
		s.Equal(s.currentOp.Id, res.Id)
	}
}

func (s *DockerLocalTestSuite) TestLocal_072_ListPullOperation() {
	res, _, err := s.e.OperationsApi.ListOperations(s.ctx, &titan.ListOperationsOpts{Repository: optional.NewString("foo")})
	if s.e.NoError(err) {
		s.Len(res, 1)
		s.Equal(s.currentOp.Id, res[0].Id)
	}
}

func (s *DockerLocalTestSuite) TestLocal_073_GetPullProgress() {
	time.Sleep(time.Duration(1) * time.Second)
	res, _, err := s.e.OperationsApi.GetOperation(s.ctx, s.currentOp.Id)
	if s.e.NoError(err) {
		s.Equal(res.State, "COMPLETE")
		progress, _, err := s.e.OperationsApi.GetOperationProgress(s.ctx, s.currentOp.Id, nil)
		if s.e.NoError(err) {
			s.Len(progress, 2)
			s.Equal("MESSAGE", progress[0].Type)
			s.Equal("Pulling id2 from 'b'", progress[0].Message)
			s.Equal("COMPLETE", progress[1].Type)
		}
	}
}

func (s *DockerLocalTestSuite) TestLocal_080_GetPulledCommit() {
	res, _, err := s.e.CommitApi.GetCommit(s.ctx, "foo", "id2")
	if s.e.NoError(err) {
		s.Equal("id2", res.Id)
	}
}

func (s *DockerLocalTestSuite) TestLocal_081_ListMultipleCommits() {
	res, _, err := s.e.CommitApi.ListCommits(s.ctx, "foo", nil)
	if s.e.NoError(err) {
		s.Len(res, 2)
	}
}

func (s *DockerLocalTestSuite) TestLocal_082_FilterOutCommit() {
	res, _, err := s.e.CommitApi.ListCommits(s.ctx, "foo", &titan.ListCommitsOpts{Tag: optional.NewInterface([]string{"a=B"})})
	if s.e.NoError(err) {
		s.Len(res, 1)
		s.Equal("id", res[0].Id)
	}
}

func (s *DockerLocalTestSuite) TestLocal_083_PushBadCommit() {
	_, _, err := s.e.OperationsApi.Push(s.ctx, "foo", "b", "id3", s.remoteParams, nil)
	s.e.APIError(err, "NoSuchObjectException")
}

func (s *DockerLocalTestSuite) TestLocal_090_AbortOperation() {
	params := titan.RemoteParameters{
		Provider:   "nop",
		Properties: map[string]interface{}{"delay": 10},
	}
	res, _, err := s.e.OperationsApi.Push(s.ctx, "foo", "b", "id", params, nil)
	if !s.e.NoError(err) {
		return
	}

	time.Sleep(time.Duration(1) * time.Second)
	_, err = s.e.OperationsApi.AbortOperation(s.ctx, res.Id)
	if !s.e.NoError(err) {
		return
	}

	time.Sleep(time.Duration(1) * time.Second)
	res, _, err = s.e.OperationsApi.GetOperation(s.ctx, res.Id)
	if s.e.NoError(err) {
		s.Equal("ABORTED", res.State)
		progress, _, err := s.e.OperationsApi.GetOperationProgress(s.ctx, res.Id, nil)
		if s.e.NoError(err) {
			s.Len(progress, 2)
			s.Equal("MESSAGE", progress[0].Type)
			s.Equal("Pushing id to 'b'", progress[0].Message)
			s.Equal("ABORT", progress[1].Type)
		}
	}
}

func (s *DockerLocalTestSuite) TestLocal_100_DeleteCommit() {
	_, err := s.e.CommitApi.DeleteCommit(s.ctx, "foo", "id2")
	s.e.NoError(err)
}

func (s *DockerLocalTestSuite) TestLocal_102_DeleteRemote() {
	_, err := s.e.RemoteApi.DeleteRemote(s.ctx, "foo", "b")
	s.e.NoError(err)
}

func (s *DockerLocalTestSuite) TestLocal_103_DeleteVolume() {
	_, err := s.e.VolumeApi.DeactivateVolume(s.ctx, "foo", "vol")
	if s.e.NoError(err) {
		_, err = s.e.VolumeApi.DeleteVolume(s.ctx, "foo", "vol")
		s.e.NoError(err)
	}
}

func (s *DockerLocalTestSuite) TestLocal_104_DeleteRepository() {
	_, err := s.e.RepoApi.DeleteRepository(s.ctx, "foo")
	s.e.NoError(err)
}
