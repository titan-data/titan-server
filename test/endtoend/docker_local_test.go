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
)

type DockerLocalTestSuite struct {
	suite.Suite
	d   *dockerUtil
	ctx context.Context

	repoApi   *titanclient.RepositoriesApiService
	remoteApi *titanclient.RemotesApiService
	volumeApi *titanclient.VolumesApiService
	commitApi *titanclient.CommitsApiService
}

func (s *DockerLocalTestSuite) SetupSuite() {
	s.d = DockerUtil("docker-zfs")
	s.d.SetupStandardDocker()
	s.ctx = context.Background()

	s.repoApi = s.d.Client.RepositoriesApi
	s.volumeApi = s.d.Client.VolumesApi
	s.remoteApi = s.d.Client.RemotesApi
	s.commitApi = s.d.Client.CommitsApi
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
	mountpoint := res.Config["mountpoint"].(string)
	idx := strings.Index(mountpoint, "/var/lib/test/mnt/")
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
	s.NotZero(res.UniqueSize)
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

/*
   "write new local value succeeds" {
       dockerUtil.writeFile("foo", "vol", "testfile", "Goodbye")
       val result = dockerUtil.readFile("foo", "vol", "testfile")
       result shouldBe "Goodbye\n"
   }

   "unmount volume succeeds" {
       volumeApi.deactivateVolume("foo", "vol")
   }

   "unmount volume is idempotent" {
       volumeApi.deactivateVolume("foo", "vol")
   }

   "checkout commit and old contents are present" {
       commitApi.checkoutCommit("foo", "id")
       volumeApi.activateVolume("foo", "vol")
       val result = dockerUtil.readFile("foo", "vol", "testfile")
       result shouldBe "Hello\n"
   }

   "volume is mounted at a new location" {
       val vol = volumeApi.getVolume("foo", "vol")
       vol.config["mountpoint"] shouldNotBe volumeMountpoint
       volumeMountpoint = vol.config["mountpoint"] as String
   }

   "get repository status indicates source commit" {
       val status = repoApi.getRepositoryStatus("foo")
       status.sourceCommit shouldBe "id"
       status.lastCommit shouldBe "id"
   }

   "add remote succeeds" {
       val result = remoteApi.createRemote("foo", remote)
       result.name shouldBe "a"
   }

   "get remote succeeds" {
       val result = remoteApi.getRemote("foo", "a")
       result.provider shouldBe "nop"
       result.name shouldBe "a"
   }

   "add duplicate remote fails" {
       val exception = shouldThrow<ClientException> {
           remoteApi.createRemote("foo", remote)
       }
       exception.code shouldBe "ObjectExistsException"
   }

   "remote shows up in list" {
       val result = remoteApi.listRemotes("foo")
       result.size shouldBe 1
       result[0].name shouldBe "a"
   }

   "list remote commits succeeds" {
       val result = remoteApi.listRemoteCommits("foo", "a", params)
       result.size shouldBe 0
   }

   "get remote commit succeeds" {
       val result = remoteApi.getRemoteCommit("foo", "a", "hash", params)
       result.id shouldBe "hash"
   }

   "update remote name succeeds" {
       remoteApi.updateRemote("foo", "a", Remote("nop", "b"))
       val result = remoteApi.getRemote("foo", "b")
       result.name shouldBe "b"
       result.provider shouldBe "nop"
   }

   "list of operations is empty" {
       val result = operationApi.listOperations("foo")
       result.size shouldBe 0
   }

   "push creates new operation" {
       currentOp = operationApi.push("foo", "b", "id", params)
       currentOp.commitId shouldBe "id"
       currentOp.remote shouldBe "b"
       currentOp.type shouldBe Operation.Type.PUSH
   }

   "get push operation succeeds" {
       val result = operationApi.getOperation(currentOp.id)
       result.id shouldBe currentOp.id
       result.commitId shouldBe currentOp.commitId
       result.remote shouldBe currentOp.remote
       result.type shouldBe currentOp.type
   }

   "list operations shows push operation" {
       val result = operationApi.listOperations("foo")
       result.size shouldBe 1
       result[0].id shouldBe currentOp.id
   }

   "get push operation progress succeeds" {
       delay(Duration.ofMillis(1000))
       val result = operationApi.getOperation(currentOp.id)
       result.state shouldBe Operation.State.COMPLETE
       val progress = operationApi.getProgress(currentOp.id, 0)
       progress.size shouldBe 2
       progress[0].type shouldBe ProgressEntry.Type.MESSAGE
       progress[0].message shouldBe "Pushing id to 'b'"
       progress[1].type shouldBe ProgressEntry.Type.COMPLETE
   }

   "push operation no longer in list of operations" {
       val result = operationApi.listOperations("foo")
       result.size shouldBe 0
   }

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

   "delete remote succeeds" {
       remoteApi.deleteRemote("foo", "b")
       val exception = shouldThrow<ClientException> {
           remoteApi.getRemote("foo", "b")
       }
       exception.code shouldBe "NoSuchObjectException"
   }
*/

func (s *DockerLocalTestSuite) TestLocal_098_DeleteNonExistentRemote() {
	_, err := s.remoteApi.DeleteRemote(s.ctx, "foo", "b")
	apiErr := s.d.GetAPIError(err)
	s.Equal("NoSuchObjectException", apiErr.Code)
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
