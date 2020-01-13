/*
 * Copyright The Titan Project Contributors
 */
package endtoend

import (
	"context"
	"github.com/antihax/optional"
	"github.com/stretchr/testify/suite"
	titanclient "github.com/titan-data/titan-client-go"
	"strings"
	"testing"
	"time"
)

type DockerLocalTestSuite struct {
	suite.Suite
	d   *dockerUtil
	ctx context.Context

	volumeMountpoint string
	remoteParams titanclient.RemoteParameters
	currentOp titanclient.Operation

	repoApi   *titanclient.RepositoriesApiService
	remoteApi *titanclient.RemotesApiService
	volumeApi *titanclient.VolumesApiService
	commitApi *titanclient.CommitsApiService
	operationsApi *titanclient.OperationsApiService
}

func (s *DockerLocalTestSuite) SetupSuite() {
	s.d = DockerUtil("docker-zfs")
	s.d.SetupStandardDocker()
	s.ctx = context.Background()

	s.repoApi = s.d.Client.RepositoriesApi
	s.volumeApi = s.d.Client.VolumesApi
	s.remoteApi = s.d.Client.RemotesApi
	s.commitApi = s.d.Client.CommitsApi
	s.operationsApi = s.d.Client.OperationsApi

	s.remoteParams = titanclient.RemoteParameters{
		Provider:   "nop",
		Properties: map[string]interface{}{},
	}
}

func (s *DockerLocalTestSuite) TearDownSuite() {
	s.d.TeardownStandardDocker()
}

func TestDockerLocalSuite(t *testing.T) {
	suite.Run(t, new(DockerLocalTestSuite))
}

func (s *DockerLocalTestSuite) TestLocal_001_GetContext() {
	res, _, _ := s.d.Client.ContextsApi.GetContext(s.ctx)
	s.Equal("docker-zfs", res.Provider)
	s.Len(res.Properties, 1)
	s.Equal("test", res.Properties["pool"])
}

func (s *DockerLocalTestSuite) TestLocal_002_EmptyRepoList() {
	res, _, _ := s.repoApi.ListRepositories(s.ctx)
	s.Len(res, 0)
}

func (s *DockerLocalTestSuite) TestLocal_003_CreateRepository() {
	res, _, _ := s.repoApi.CreateRepository(s.ctx, titanclient.Repository{
		Name:       "foo",
		Properties: map[string]interface{}{"a": "b"},
	})
	s.Equal("foo", res.Name)
	s.Len(res.Properties, 1)
	s.Equal("b", res.Properties["a"])
}

func (s *DockerLocalTestSuite) TestLocal_004_GetRepository() {
	res, _, _ := s.repoApi.GetRepository(s.ctx, "foo")
	s.Equal("foo", res.Name)
	s.Len(res.Properties, 1)
	s.Equal("b", res.Properties["a"])
}

func (s *DockerLocalTestSuite) TestLocal_005_ListRepositoryPresent() {
	res, _, _ := s.repoApi.ListRepositories(s.ctx)
	s.Len(res, 1)
	repo := res[0]
	s.Equal("foo", repo.Name)
	s.Len(repo.Properties, 1)
	s.Equal("b", repo.Properties["a"])
}

func (s *DockerLocalTestSuite) TestLocal_006_CreateDuplicate() {
	_, _, err := s.repoApi.CreateRepository(s.ctx, titanclient.Repository{
		Name:       "foo",
		Properties: map[string]interface{}{},
	})
	apiErr := s.d.GetAPIError(err)
	s.Equal("ObjectExistsException", apiErr.Code)
}

func (s *DockerLocalTestSuite) TestLocal_010_CreateVolume() {
	res, _, _ := s.volumeApi.CreateVolume(s.ctx, "foo", titanclient.Volume{
		Name:       "vol",
		Properties: map[string]interface{}{"a": "b"},
	})
	s.Equal("vol", res.Name)
	s.Len(res.Properties, 1)
	s.Equal("b", res.Properties["a"])
}

func (s *DockerLocalTestSuite) TestLocal_011_CreateVolumeBadRepo() {
	_, _, err := s.volumeApi.CreateVolume(s.ctx, "bar", titanclient.Volume{
		Name:       "vol",
		Properties: map[string]interface{}{"a": "b"},
	})
	apiErr := s.d.GetAPIError(err)
	s.Equal("NoSuchObjectException", apiErr.Code)
}

func (s *DockerLocalTestSuite) TestLocal_012_CreateVolumeDuplicate() {
	_, _, err := s.volumeApi.CreateVolume(s.ctx, "foo", titanclient.Volume{
		Name:       "vol",
		Properties: map[string]interface{}{"a": "b"},
	})
	apiErr := s.d.GetAPIError(err)
	s.Equal("ObjectExistsException", apiErr.Code)
}

func (s *DockerLocalTestSuite) TestLocal_013_GetVolume() {
	res, _, _ := s.volumeApi.GetVolume(s.ctx, "foo", "vol")
	s.Equal("vol", res.Name)
	s.Len(res.Properties, 1)
	s.Equal("b", res.Properties["a"])
	s.volumeMountpoint = res.Config["mountpoint"].(string)
	idx := strings.Index(s.volumeMountpoint, "/var/lib/test/mnt/")
	s.Equal(0, idx)
}

func (s *DockerLocalTestSuite) TestLocal_014_GetBadVolume() {
	_, _, err := s.volumeApi.GetVolume(s.ctx, "bar", "vol")
	apiErr := s.d.GetAPIError(err)
	s.Equal("NoSuchObjectException", apiErr.Code)
}

func (s *DockerLocalTestSuite) TestLocal_015_ListVolume() {
	res, _, _ := s.volumeApi.ListVolumes(s.ctx, "foo")
	s.Len(res, 1)
	s.Equal("vol", res[0].Name)
}

func (s *DockerLocalTestSuite) TestLocal_016_MountVolume() {
	_, err := s.volumeApi.ActivateVolume(s.ctx, "foo", "vol")
	s.Nil(err)
}

func (s *DockerLocalTestSuite) TestLocal_017_CreateFile() {
	err := s.d.WriteFile("foo", "vol", "testfile", "Hello")
	s.Nil(err)
	res, _ := s.d.ReadFile("foo", "vol", "testfile")
	s.Equal("Hello", res)
}

func (s *DockerLocalTestSuite) TestLocal_020_LastCommitEmpty() {
	res, _, _ := s.repoApi.GetRepositoryStatus(s.ctx, "foo")
	s.Empty(res.SourceCommit)
	s.Empty(res.LastCommit)
}

func (s *DockerLocalTestSuite) TestLocal_021_CreateCommit() {
	res, _, _ := s.commitApi.CreateCommit(s.ctx, "foo", titanclient.Commit{
		Id: "id",
		Properties: map[string]interface{}{"tags": map[string]string{
			"a": "b",
			"c": "d",
		}},
	})
	s.Equal("id", res.Id)
	s.Equal("b", s.d.GetTag(res, "a"))
}

func (s *DockerLocalTestSuite) TestLocal_022_DuplicateCommit() {
	_, _, err := s.commitApi.CreateCommit(s.ctx, "foo", titanclient.Commit{
		Id:         "id",
		Properties: map[string]interface{}{},
	})
	apiErr := s.d.GetAPIError(err)
	s.Equal("ObjectExistsException", apiErr.Code)
}

func (s *DockerLocalTestSuite) TestLocal_023_GetCommit() {
	res, _, _ := s.commitApi.GetCommit(s.ctx, "foo", "id")
	s.Equal("id", res.Id)
	s.Equal("b", s.d.GetTag(res, "a"))
}

func (s *DockerLocalTestSuite) TestLocal_024_GetBadCommit() {
	_, _, err := s.commitApi.GetCommit(s.ctx, "foo", "id2")
	apiErr := s.d.GetAPIError(err)
	s.Equal("NoSuchObjectException", apiErr.Code)
}

func (s *DockerLocalTestSuite) TestLocal_025_UpdateCommit() {
	res, _, _ := s.commitApi.UpdateCommit(s.ctx, "foo", "id", titanclient.Commit{
		Id: "id",
		Properties: map[string]interface{}{"tags": map[string]string{
			"a": "B",
			"c": "d",
		}},
	})
	s.Equal("id", res.Id)
	s.Equal("B", s.d.GetTag(res, "a"))
	res, _, _ = s.commitApi.GetCommit(s.ctx, "foo", "id")
	s.Equal("B", s.d.GetTag(res, "a"))
}

func (s *DockerLocalTestSuite) TestLocal_026_CommitStatus() {
	res, _, _ := s.commitApi.GetCommitStatus(s.ctx, "foo", "id")
	s.NotZero(res.LogicalSize)
	s.NotZero(res.ActualSize)
}

func (s *DockerLocalTestSuite) TestLocal_027_DeleteBadCommit() {
	_, err := s.commitApi.DeleteCommit(s.ctx, "foo", "id2")
	apiErr := s.d.GetAPIError(err)
	s.Equal("NoSuchObjectException", apiErr.Code)
}

func (s *DockerLocalTestSuite) TestLocal_030_ListCommit() {
	res, _, _ := s.commitApi.ListCommits(s.ctx, "foo", nil)
	s.Len(res, 1)
	s.Equal("id", res[0].Id)
}

func (s *DockerLocalTestSuite) TestLocal_031_FilterOut() {
	res, _, _ := s.commitApi.ListCommits(s.ctx, "foo", &titanclient.ListCommitsOpts{
		Tag: optional.NewInterface([]string{"a=c"}),
	})
	s.Len(res, 0)
}

func (s *DockerLocalTestSuite) TestLocal_032_FilterPresent() {
	res, _, _ := s.commitApi.ListCommits(s.ctx, "foo", &titanclient.ListCommitsOpts{
		Tag: optional.NewInterface([]string{"a=B"}),
	})
	s.Len(res, 1)
	s.Equal("id", res[0].Id)
}

func (s *DockerLocalTestSuite) TestLocal_033_FilterCompound() {
	res, _, _ := s.commitApi.ListCommits(s.ctx, "foo", &titanclient.ListCommitsOpts{
		Tag: optional.NewInterface([]string{"a=B", "c"}),
	})
	s.Len(res, 1)
	s.Equal("id", res[0].Id)
}

func (s *DockerLocalTestSuite) TestLocal_034_RepositoryStatus() {
	res, _, _ := s.repoApi.GetRepositoryStatus(s.ctx, "foo")
	s.Equal("id", res.SourceCommit)
	s.Equal("id", res.LastCommit)
}

func (s *DockerLocalTestSuite) TestLocal_040_VolumeStatus() {
	res, _, _ := s.volumeApi.GetVolumeStatus(s.ctx, "foo", "vol")
	s.Equal("vol", res.Name)
	s.NotZero(res.ActualSize)
	s.NotZero(res.LogicalSize)
	s.True(res.Ready)
	s.Empty(res.Error)
}

func (s *DockerLocalTestSuite) TestLocal_041_WriteNewValue() {
	err := s.d.WriteFile("foo", "vol", "testfile", "Goodbye")
	s.Nil(err)
	res, _ := s.d.ReadFile("foo", "vol", "testfile")
	s.Equal("Goodbye", res)
}

func (s *DockerLocalTestSuite) TestLocal_042_Unmount() {
	_, err := s.volumeApi.DeactivateVolume(s.ctx, "foo", "vol")
	s.Nil(err)
}

func (s *DockerLocalTestSuite) TestLocal_043_UnmountIdempotent() {
	_, err := s.volumeApi.DeactivateVolume(s.ctx, "foo", "vol")
	s.Nil(err)
}

func (s *DockerLocalTestSuite) TestLocal_044_Checkout() {
	_, err := s.commitApi.CheckoutCommit(s.ctx, "foo", "id")
	s.Nil(err)
	_, err = s.volumeApi.ActivateVolume(s.ctx, "foo", "vol")
	s.Nil(err)
	res, _ := s.d.ReadFile("foo", "vol", "testfile")
	s.Equal("Hello", res)
}

func (s *DockerLocalTestSuite) TestLocal_045_NewMountpoint() {
	res, _, _ := s.volumeApi.GetVolume(s.ctx, "foo", "vol")
	s.NotEqual(s.volumeMountpoint, res.Config["mountpoint"])
	s.volumeMountpoint = res.Config["mountpoint"].(string)
}

func (s *DockerLocalTestSuite) TestLocal_046_SourceCommit() {
	res, _, _ := s.repoApi.GetRepositoryStatus(s.ctx, "foo")
	s.Equal("id", res.SourceCommit)
	s.Equal("id", res.LastCommit)
}

func (s *DockerLocalTestSuite) TestLocal_050_AddRemote() {
	res, _, _ := s.remoteApi.CreateRemote(s.ctx, "foo", titanclient.Remote{
		Provider:   "nop",
		Name:       "a",
		Properties: map[string]interface{}{},
	})
	s.Equal("a", res.Name)
}

func (s *DockerLocalTestSuite) TestLocal_051_GetRemote() {
	res, _, _ := s.remoteApi.GetRemote(s.ctx, "foo", "a")
	s.Equal("nop", res.Provider)
	s.Equal("a", res.Name)
}

func (s *DockerLocalTestSuite) TestLocal_052_DuplicateRemote() {
	_, _, err := s.remoteApi.CreateRemote(s.ctx, "foo", titanclient.Remote{
		Provider:   "nop",
		Name:       "a",
		Properties: map[string]interface{}{},
	})
	apiErr := s.d.GetAPIError(err)
	s.Equal("ObjectExistsException", apiErr.Code)
}

func (s *DockerLocalTestSuite) TestLocal_053_ListRemotes() {
	res, _, _ := s.remoteApi.ListRemotes(s.ctx, "foo")
	s.Len(res, 1)
	s.Equal("a", res[0].Name)
}

func (s *DockerLocalTestSuite) TestLocal_054_ListRemoteCommits() {
	res, _, _ := s.remoteApi.ListRemoteCommits(s.ctx, "foo", "a", s.remoteParams, nil)
	s.Len(res, 0)
}

func (s *DockerLocalTestSuite) TestLocal_055_GetRemoteCommit() {
	res, _, _ := s.remoteApi.GetRemoteCommit(s.ctx, "foo", "a", "hash", s.remoteParams)
	s.Equal("hash", res.Id)
}

func (s *DockerLocalTestSuite) TestLocal_056_DeleteNonExistentRemote() {
	_, err := s.remoteApi.DeleteRemote(s.ctx, "foo", "b")
	apiErr := s.d.GetAPIError(err)
	s.Equal("NoSuchObjectException", apiErr.Code)
}

func (s *DockerLocalTestSuite) TestLocal_057_UpdateRemote() {
	_, _, _ = s.remoteApi.UpdateRemote(s.ctx, "foo", "a", titanclient.Remote{
		Provider:   "nop",
		Name:       "b",
		Properties: map[string]interface{}{},
	})
	res, _, _ := s.remoteApi.GetRemote(s.ctx, "foo", "b")
	s.Equal("nop", res.Provider)
	s.Equal("b", res.Name)
}

func (s *DockerLocalTestSuite) TestLocal_060_ListEmptyOperations() {
	res, _, _ := s.operationsApi.ListOperations(s.ctx, nil)
	s.Len(res, 0)
}

func (s *DockerLocalTestSuite) TestLocal_061_StartPush() {
	res, _, _ := s.operationsApi.Push(s.ctx, "foo", "b", "id", nil)
	s.Equal("id", res.Id)
	s.Equal("PUSH", res.Type)
	s.Equal("b", res.Remote)
	s.currentOp = res
}

func (s *DockerLocalTestSuite) TestLocal_062_GetOperation() {
	res, _, _ := s.operationsApi.GetOperation(s.ctx, s.currentOp.Id)
	s.Equal("id", res.Id)
	s.Equal("PUSH", res.Type)
	s.Equal("b", res.Remote)
	s.Equal(s.currentOp.CommitId, res.CommitId)
}

func (s *DockerLocalTestSuite) TestLocal_063_ListOperations() {
	res, _, _ := s.operationsApi.ListOperations(s.ctx, &titanclient.ListOperationsOpts{Repository: optional.NewString("foo")})
	s.Len(res, 1)
	s.Equal(s.currentOp.Id, res[0].Id)
}

func (s *DockerLocalTestSuite) TestLocal_064_GetPushProgress() {
	time.Sleep(time.Duration(1) * time.Second)
	res, _, _ := s.operationsApi.GetOperation(s.ctx, s.currentOp.Id)
	s.Equal(res.State, "COMPLETE")
	progress, _, _ := s.operationsApi.GetOperationProgress(s.ctx, s.currentOp.Id, nil)
	s.Len(progress, 2)
	s.Equal("MESSAGE", progress[0].Type)
	s.Equal("Pushing id to 'b'", progress[0].Message)
	s.Equal("COMPLETE", progress[1].Type)
}

func (s *DockerLocalTestSuite) TestLocal_065_ListNotPresent() {
	res, _, _ := s.operationsApi.ListOperations(s.ctx, &titanclient.ListOperationsOpts{Repository: optional.NewString("foo")})
	s.Len(res, 0)
}

/*
   "pull creates new operation" {
       currentOp = operationApi.pull("foo", "b", "id2", params)
       currentOp.commitId shouldBe "id2"
       currentOp.remote shouldBe "b"
       currentOp.type shouldBe Operation.Type.PULL
   }

   "get pull operation succeeds" {
       val result = operationApi.getOperation(currentOp.id)
       result.id shouldBe currentOp.id
       result.commitId shouldBe currentOp.commitId
       result.remote shouldBe currentOp.remote
       result.type shouldBe currentOp.type
   }

   "list operations shows pull operation" {
       val result = operationApi.listOperations("foo")
       result.size shouldBe 1
       result[0].id shouldBe currentOp.id
   }

   "get pull progress succeeds" {
       delay(Duration.ofMillis(1000))
       val result = operationApi.getOperation(currentOp.id)
       result.state shouldBe Operation.State.COMPLETE
       val progress = operationApi.getProgress(currentOp.id, 0)
       progress.size shouldBe 2
       progress[0].type shouldBe ProgressEntry.Type.MESSAGE
       progress[0].message shouldBe "Pulling id2 from 'b'"
       progress[1].type shouldBe ProgressEntry.Type.COMPLETE
   }

   "pulled commit exists" {
       val commit = commitApi.getCommit("foo", "id2")
       commit.id shouldBe "id2"
   }

   "list commits shows two commits" {
       val commits = commitApi.listCommits("foo")
       commits.size shouldBe 2
   }

   "list commits filters out commit" {
       val commits = commitApi.listCommits("foo", listOf("a=B"))
       commits.size shouldBe 1
       commits[0].id shouldBe "id"
   }

   "push non-existent commit fails" {
       val exception = shouldThrow<ClientException> {
           operationApi.push("foo", "b", "id3", params)
       }
       exception.code shouldBe "NoSuchObjectException"
   }

   "aborted operation is marked aborted" {
       val props = params.properties.toMutableMap()
       props["delay"] = 10
       currentOp = operationApi.push("foo", "b", "id", RemoteParameters(params.provider, props))
       currentOp.state shouldBe Operation.State.RUNNING
       delay(Duration.ofMillis(1000))
       operationApi.deleteOperation(currentOp.id)
       delay(Duration.ofMillis(1000))
       val result = operationApi.getOperation(currentOp.id)
       result.state shouldBe Operation.State.ABORTED
       val progress = operationApi.getProgress(currentOp.id, 0)
       progress.size shouldBe 2
       progress[0].type shouldBe ProgressEntry.Type.MESSAGE
       progress[0].message shouldBe "Pushing id to 'b'"
       progress[1].type shouldBe ProgressEntry.Type.ABORT
   }
*/

func (s *DockerLocalTestSuite) TestLocal_098_DeleteRemote() {
	_, err := s.remoteApi.DeleteRemote(s.ctx, "foo", "a")
	s.Nil(err)
}

func (s *DockerLocalTestSuite) TestLocal_099_DeleteVolume() {
	_, err := s.volumeApi.DeactivateVolume(s.ctx, "foo", "vol")
	s.Nil(err)
	_, err = s.volumeApi.DeleteVolume(s.ctx, "foo", "vol")
	s.Nil(err)
}

func (s *DockerLocalTestSuite) TestLocal_100_DeleteRepository() {
	_, err := s.repoApi.DeleteRepository(s.ctx, "foo")
	s.Nil(err)
}
