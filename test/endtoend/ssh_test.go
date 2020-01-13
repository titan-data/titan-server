/*
 * Copyright The Titan Project Contributors
 */
package endtoend

import (
	"context"
	"github.com/stretchr/testify/suite"
	titanclient "github.com/titan-data/titan-client-go"
	"testing"
)

type SshTestSuite struct {
	suite.Suite
	e   *EndToEndTest
	ctx context.Context

	volumeMountpoint string
	remoteParams     titanclient.RemoteParameters
	currentOp        titanclient.Operation

	repoApi       *titanclient.RepositoriesApiService
	remoteApi     *titanclient.RemotesApiService
	volumeApi     *titanclient.VolumesApiService
	commitApi     *titanclient.CommitsApiService
	operationsApi *titanclient.OperationsApiService
}

func (s *SshTestSuite) SetupSuite() {
	s.e = NewEndToEndTest(&s.Suite, "docker-zfs")
	s.e.SetupStandardDocker()
	s.e.SetupStandardSsh()

	s.ctx = context.Background()

	s.repoApi = s.e.Client.RepositoriesApi
	s.volumeApi = s.e.Client.VolumesApi
	s.remoteApi = s.e.Client.RemotesApi
	s.commitApi = s.e.Client.CommitsApi
	s.operationsApi = s.e.Client.OperationsApi

	s.remoteParams = titanclient.RemoteParameters{
		Provider:   "nop",
		Properties: map[string]interface{}{},
	}
}

func (s *SshTestSuite) TearDownSuite() {
	s.e.TeardownStandardDocker()
	s.e.TeardownStandardSsh()
}

func TestSshTestSuite(t *testing.T) {
	suite.Run(t, new(SshTestSuite))
}

func (s *SshTestSuite) TestSsh_001_CreateRepository() {
	_, _, err := s.repoApi.CreateRepository(s.ctx, titanclient.Repository{
		Name:       "foo",
		Properties: map[string]interface{}{},
	})
	s.e.NoError(err)
}

func (s *SshTestSuite) TestSsh_002_CreateMountVolume() {
	_, _, err := s.volumeApi.CreateVolume(s.ctx, "foo", titanclient.Volume{
		Name:       "vol",
		Properties: map[string]interface{}{},
	})
	if s.e.NoError(err) {
		_, err := s.volumeApi.ActivateVolume(s.ctx, "foo", "vol")
		s.e.NoError(err)
	}
}

func (s *SshTestSuite) TestSsh_003_CreateFile() {
	err := s.e.WriteFile("foo", "vol", "testfile", "Hello")
	if s.e.NoError(err) {
		res, err := s.e.ReadFile("foo", "vol", "testfile")
		if s.e.NoError(err) {
			s.Equal("Hello", res)
		}
	}
}

func (s *SshTestSuite) TestSsh_004_CreateCommit() {
	res, _, err := s.commitApi.CreateCommit(s.ctx, "foo", titanclient.Commit{
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

/*
   "add ssh remote succeeds" {
       dockerUtil.mkdirSsh("/bar")

       val remote = RemoteUtil().parseUri("${dockerUtil.getSshUri()}/bar", "origin", mapOf())
       remote.properties["address"] shouldBe dockerUtil.getSshHost()
       remote.properties["password"] shouldBe "test"
       remote.properties["username"] shouldBe "test"
       remote.properties["port"] shouldBe 22
       remote.name shouldBe "origin"

       remoteApi.createRemote("foo", remote)
   }

   "list remote commits returns empty list" {
       val commits = remoteApi.listRemoteCommits("foo", "origin", params)
       commits.size shouldBe 0
   }

   "get non-existent remote commit fails" {
       val exception = shouldThrow<ClientException> {
           remoteApi.getRemoteCommit("foo", "origin", "id", params)
       }
       exception.code shouldBe "NoSuchObjectException"
   }

   "push commit succeeds" {
       val op = operationApi.push("foo", "origin", "id", params)
       waitForOperation(op.id)
   }

   "list remote commits returns pushed commit" {
       val commits = remoteApi.listRemoteCommits("foo", "origin", params)
       commits.size shouldBe 1
       commits[0].id shouldBe "id"
       getTag(commits[0], "a") shouldBe "b"
   }

   "list remote commits filters out commit" {
       val commits = remoteApi.listRemoteCommits("foo", "origin", params, listOf("e"))
       commits.size shouldBe 0
   }

   "list remote commits filters include commit" {
       val commits = remoteApi.listRemoteCommits("foo", "origin", params, listOf("a=b", "c=d"))
       commits.size shouldBe 1
       commits[0].id shouldBe "id"
   }

   "remote file contents is correct" {
       val content = dockerUtil.readFileSsh("/bar/id/data/vol/testfile")
       content shouldBe "Hello\n"
   }

   "push of same commit fails" {
       val exception = shouldThrow<ClientException> {
           operationApi.push("foo", "origin", "id", params)
       }
       exception.code shouldBe "ObjectExistsException"
   }

   "update commit succeeds" {
       val newCommit = Commit(id = "id", properties = mapOf("tags" to mapOf("a" to "B", "c" to "d")))
       commitApi.updateCommit("foo", newCommit)
       getTag(newCommit, "a") shouldBe "B"
       val commit = commitApi.getCommit("foo", "id")
       getTag(commit, "a") shouldBe "B"
   }

   "push commit metadata succeeds" {
       val op = operationApi.push("foo", "origin", "id", params, true)
       waitForOperation(op.id)
   }

   "remote commit metadata updated" {
       val commit = commitApi.getCommit("foo", "id")
       commit.id shouldBe "id"
       getTag(commit, "a") shouldBe "B"
       getTag(commit, "c") shouldBe "d"
   }

   "delete local commit succeeds" {
       commitApi.deleteCommit("foo", "id")
   }

   "list local commits is empty" {
       val result = commitApi.listCommits("foo")
       result.size shouldBe 0
   }

   "write new local value succeeds" {
       dockerUtil.writeFile("foo", "vol", "testfile", "Goodbye")
       val result = dockerUtil.readFile("foo", "vol", "testfile")
       result shouldBe "Goodbye\n"
   }

   "pull original commit succeeds" {
       val op = operationApi.pull("foo", "origin", "id", params)
       waitForOperation(op.id)
   }

   "pull same commit fails" {
       val exception = shouldThrow<ClientException> {
           operationApi.pull("foo", "origin", "id", params)
       }
       exception.code shouldBe "ObjectExistsException"
   }

   "pull of metadata only succeeds" {
       val op = operationApi.pull("foo", "origin", "id", params, true)
       waitForOperation(op.id)
   }

   "checkout commit succeeds" {
       volumeApi.deactivateVolume("foo", "vol")
       commitApi.checkoutCommit("foo", "id")
       volumeApi.activateVolume("foo", "vol")
   }

   "original file contents are present" {
       val result = dockerUtil.readFile("foo", "vol", "testfile")
       result shouldBe "Hello\n"
   }

   "remove remote succeeds" {
       remoteApi.deleteRemote("foo", "origin")
   }

   "add remote without password succeeds" {
       val remote = Remote("ssh", "origin", mapOf("address" to dockerUtil.getSshHost(), "username" to "test",
               "path" to "/bar"))
       remote.properties["address"] shouldBe dockerUtil.getSshHost()
       remote.properties["password"] shouldBe null
       remote.properties["username"] shouldBe "test"
       remote.properties["port"] shouldBe null
       remote.name shouldBe "origin"

       remoteApi.createRemote("foo", remote)
   }

   "list commits with password succeeds" {
       val commits = remoteApi.listRemoteCommits("foo", "origin", RemoteParameters("ssh", mapOf("password" to "test")))
       commits.size shouldBe 1
       commits[0].id shouldBe "id"
   }

   "list commits without password fails" {
       val exception = shouldThrow<ClientException> {
           remoteApi.listRemoteCommits("foo", "origin", params)
       }
       exception.code shouldBe "IllegalArgumentException"
   }

   "list commits with incorrect password fails" {
       val exception = shouldThrow<ServerException> {
           remoteApi.listRemoteCommits("foo", "origin", RemoteParameters("ssh", mapOf("password" to "r00t")))
       }
       exception.code shouldBe "CommandException"
   }

   "copy SSH key to server succeeds" {
       val key = getResource("/id_rsa.pub")
       dockerUtil.writeFileSsh("/home/test/.ssh/authorized_keys", key)
   }

   "list commits with key succeeds" {
       val key = getResource("/id_rsa")
       val commits = remoteApi.listRemoteCommits("foo", "origin", RemoteParameters("ssh", mapOf("key" to key)))
       commits.size shouldBe 1
       commits[0].id shouldBe "id"
   }

   "pull commit with key succeeds" {
       val key = getResource("/id_rsa")
       commitApi.deleteCommit("foo", "id")
       val op = operationApi.pull("foo", "origin", "id", RemoteParameters("ssh", mapOf("key" to key)))
       waitForOperation(op.id)
   }

*/

func (s *SshTestSuite) TestSsh_103_DeleteVolume() {
	_, err := s.volumeApi.DeactivateVolume(s.ctx, "foo", "vol")
	if s.e.NoError(err) {
		_, err = s.volumeApi.DeleteVolume(s.ctx, "foo", "vol")
		s.e.NoError(err)
	}
}

func (s *SshTestSuite) TestSsh_104_DeleteRepository() {
	_, err := s.repoApi.DeleteRepository(s.ctx, "foo")
	s.e.NoError(err)
}
