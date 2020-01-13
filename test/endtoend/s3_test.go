/*
 * Copyright The Titan Project Contributors
 */
package endtoend

import (
	"context"
	"github.com/antihax/optional"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/suite"
	titanclient "github.com/titan-data/titan-client-go"
	"os"
	"strings"
	"testing"
)

type S3TestSuite struct {
	suite.Suite
	e   *EndToEndTest
	ctx context.Context

	s3bucket     string
	s3path       string
	remote       titanclient.Remote
	remoteParams titanclient.RemoteParameters
	currentOp    titanclient.Operation

	repoApi       *titanclient.RepositoriesApiService
	remoteApi     *titanclient.RemotesApiService
	volumeApi     *titanclient.VolumesApiService
	commitApi     *titanclient.CommitsApiService
	operationsApi *titanclient.OperationsApiService
}

func (s *S3TestSuite) ClearBucket() error {
	sess, err := session.NewSessionWithOptions(session.Options{SharedConfigState: session.SharedConfigEnable})
	if err != nil {
		return err
	}
	svc := s3.New(sess)
	res, err := svc.ListObjects(&s3.ListObjectsInput{Bucket: aws.String(s.s3bucket), Prefix: aws.String(s.s3path)})
	if err != nil {
		return err
	}
	for _, obj := range res.Contents {
		_, err = svc.DeleteObject(&s3.DeleteObjectInput{
			Bucket: aws.String(s.s3bucket),
			Key:    obj.Key,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *S3TestSuite) SetupSuite() {
	location := os.Getenv("S3_LOCATION")
	if location == "" {
		panic("S3_LOCATION must be set in environment")
	}
	s.s3bucket = location[:strings.IndexByte(location, '/')]
	s.s3path = location[strings.IndexByte(location, '/')+1:]
	err := s.ClearBucket()
	if err != nil {
		panic(err)
	}

	sess, err := session.NewSessionWithOptions(session.Options{SharedConfigState: session.SharedConfigEnable})
	if err != nil {
		panic(err)
	}
	creds, err := sess.Config.Credentials.Get()
	if err != nil {
		panic(err)
	}

	s.remote = titanclient.Remote{
		Provider: "s3",
		Name:     "origin",
		Properties: map[string]interface{}{
			"bucket":    s.s3bucket,
			"path":      s.s3path,
			"accessKey": creds.AccessKeyID,
			"secretKey": creds.SecretAccessKey,
			"region":    sess.Config.Region,
		},
	}

	s.e = NewEndToEndTest(&s.Suite, "docker-zfs")
	s.e.SetupStandardDocker()

	s.ctx = context.Background()

	s.repoApi = s.e.Client.RepositoriesApi
	s.volumeApi = s.e.Client.VolumesApi
	s.remoteApi = s.e.Client.RemotesApi
	s.commitApi = s.e.Client.CommitsApi
	s.operationsApi = s.e.Client.OperationsApi

	s.remoteParams = titanclient.RemoteParameters{
		Provider:   "s3",
		Properties: map[string]interface{}{},
	}
}

func (s *S3TestSuite) TearDownSuite() {
	s.e.TeardownStandardDocker()
}

func TestS3TestSuite(t *testing.T) {
	suite.Run(t, new(S3TestSuite))
}

func (s *S3TestSuite) TestS3_001_CreateRepository() {
	_, _, err := s.repoApi.CreateRepository(s.ctx, titanclient.Repository{
		Name:       "foo",
		Properties: map[string]interface{}{},
	})
	s.e.NoError(err)
}

func (s *S3TestSuite) TestS3_002_CreateMountVolume() {
	_, _, err := s.volumeApi.CreateVolume(s.ctx, "foo", titanclient.Volume{
		Name:       "vol",
		Properties: map[string]interface{}{},
	})
	if s.e.NoError(err) {
		_, err := s.volumeApi.ActivateVolume(s.ctx, "foo", "vol")
		s.e.NoError(err)
	}
}

func (s *S3TestSuite) TestS3_003_CreateFile() {
	err := s.e.WriteFile("foo", "vol", "testfile", "Hello")
	if s.e.NoError(err) {
		res, err := s.e.ReadFile("foo", "vol", "testfile")
		if s.e.NoError(err) {
			s.Equal("Hello", res)
		}
	}
}

func (s *S3TestSuite) TestS3_004_CreateCommit() {
	res, _, err := s.commitApi.CreateCommit(s.ctx, "foo", titanclient.Commit{
		Id: "id",
		Properties: map[string]interface{}{"tags": map[string]string{
			"a": "b",
			"c": "d",
		}},
	})
	if s.e.NoError(err) {
		s.Equal("id", res.Id)
		s.Equal("b", s.e.GetTag(res, "a"))
	}
}

func (s *S3TestSuite) TestS3_005_AddRemote() {
	res, _, err := s.remoteApi.CreateRemote(s.ctx, "foo", s.remote)
	if s.e.NoError(err) {
		s.Equal("origin", res.Name)
		s.Equal(s.s3bucket, res.Properties["bucket"])
		s.Equal(s.s3path, res.Properties["path"])
	}
}

func (s *S3TestSuite) TestS3_010_ListEmptyRemoteCommits() {
	res, _, err := s.remoteApi.ListRemoteCommits(s.ctx, "foo", "origin", s.remoteParams, nil)
	if s.e.NoError(err) {
		s.Len(res, 0)
	}
}

func (s *S3TestSuite) TestS3_011_GetBadRemoteCommit() {
	_, _, err := s.remoteApi.GetRemoteCommit(s.ctx, "foo", "origin", "id2", s.remoteParams)
	s.e.APIError(err, "NoSuchObjectException")
}

func (s *S3TestSuite) TestS3_020_PushCommit() {
	res, _, err := s.operationsApi.Push(s.ctx, "foo", "origin", "id", s.remoteParams, nil)
	if s.e.NoError(err) {
		_, err = s.e.WaitForOperation(res.Id)
		s.e.NoError(err)
	}
}

func (s *S3TestSuite) TestS3_021_ListRemoteCommit() {
	res, _, err := s.remoteApi.ListRemoteCommits(s.ctx, "foo", "origin", s.remoteParams, nil)
	if s.e.NoError(err) {
		s.Len(res, 1)
		s.Equal("id", res[0].Id)
		s.Equal("b", s.e.GetTag(res[0], "a"))
	}
}

func (s *S3TestSuite) TestS3_022_ListRemoteFilterOut() {
	res, _, err := s.remoteApi.ListRemoteCommits(s.ctx, "foo", "origin", s.remoteParams,
		&titanclient.ListRemoteCommitsOpts{Tag: optional.NewInterface([]string{"e"})})
	if s.e.NoError(err) {
		s.Len(res, 0)
	}
}

func (s *S3TestSuite) TestS3_023_ListRemoteFilterInclude() {
	res, _, err := s.remoteApi.ListRemoteCommits(s.ctx, "foo", "origin", s.remoteParams,
		&titanclient.ListRemoteCommitsOpts{Tag: optional.NewInterface([]string{"a=b", "c=d"})})
	if s.e.NoError(err) {
		s.Len(res, 1)
		s.Equal("id", res[0].Id)
	}
}

func (s *S3TestSuite) TestS3_030_PushDuplicateCommit() {
	_, _, err := s.operationsApi.Push(s.ctx, "foo", "origin", "id", s.remoteParams, nil)
	s.e.APIError(err, "ObjectExistsException")
}

func (s *S3TestSuite) TestS3_031_UpdateCommit() {
	res, _, err := s.commitApi.UpdateCommit(s.ctx, "foo", "id", titanclient.Commit{
		Id: "id",
		Properties: map[string]interface{}{"tags": map[string]string{
			"a": "B",
			"c": "e",
		}},
	})
	if s.e.NoError(err) {
		s.Equal("B", s.e.GetTag(res, "a"))
	}
}

func (s *S3TestSuite) TestS3_032_PushMedata() {
	res, _, err := s.operationsApi.Push(s.ctx, "foo", "origin", "id", s.remoteParams,
		&titanclient.PushOpts{MetadataOnly: optional.NewBool(true)})
	if s.e.NoError(err) {
		_, err = s.e.WaitForOperation(res.Id)
		s.e.NoError(err)
	}
}

func (s *S3TestSuite) TestS3_033_RemoteMetadataUpdated() {
	res, _, err := s.remoteApi.GetRemoteCommit(s.ctx, "foo", "origin", "id", s.remoteParams)
	if s.e.NoError(err) {
		s.Equal("id", res.Id)
		s.Equal("B", s.e.GetTag(res, "a"))
	}
}

func (s *S3TestSuite) TestS3_040_DeleteLocalCommit() {
	_, err := s.commitApi.DeleteCommit(s.ctx, "foo", "id")
	s.e.NoError(err)
}

func (s *S3TestSuite) TestS3_041_ListEmptyCommits() {
	res, _, err := s.commitApi.ListCommits(s.ctx, "foo", nil)
	if s.e.NoError(err) {
		s.Len(res, 0)
	}
}

func (s *S3TestSuite) TestS3_042_UpdateFile() {
	err := s.e.WriteFile("foo", "vol", "testfile", "Goodbye")
	if s.e.NoError(err) {
		res, err := s.e.ReadFile("foo", "vol", "testfile")
		if s.e.NoError(err) {
			s.Equal("Goodbye", res)
		}
	}
}

func (s *S3TestSuite) TestS3_043_PullCommit() {
	res, _, err := s.operationsApi.Pull(s.ctx, "foo", "origin", "id", s.remoteParams, nil)
	if s.e.NoError(err) {
		_, err = s.e.WaitForOperation(res.Id)
		s.e.NoError(err)
	}
}

func (s *S3TestSuite) TestS3_044_PullDuplicate() {
	_, _, err := s.operationsApi.Pull(s.ctx, "foo", "origin", "id", s.remoteParams, nil)
	s.e.APIError(err, "ObjectExistsException")
}

func (s *S3TestSuite) TestS3_045_PullMetadata() {
	res, _, err := s.operationsApi.Pull(s.ctx, "foo", "origin", "id", s.remoteParams,
		&titanclient.PullOpts{MetadataOnly: optional.NewBool(true)})
	if s.e.NoError(err) {
		_, err = s.e.WaitForOperation(res.Id)
		s.e.NoError(err)
	}
}

func (s *S3TestSuite) TestS3_046_CheckoutCommit() {
	_, err := s.volumeApi.DeactivateVolume(s.ctx, "foo", "vol")
	if s.e.NoError(err) {
		_, err := s.commitApi.CheckoutCommit(s.ctx, "foo", "id")
		if s.e.NoError(err) {
			_, err = s.volumeApi.ActivateVolume(s.ctx, "foo", "vol")
			s.e.NoError(err)
		}
	}
}

func (s *S3TestSuite) TestS3_047_OriginalContents() {
	res, err := s.e.ReadFile("foo", "vol", "testfile")
	if s.e.NoError(err) {
		s.Equal("Hello", res)
	}
}

func (s *S3TestSuite) TestS3_050_RemoveRemote() {
	_, err := s.remoteApi.DeleteRemote(s.ctx, "foo", "origin")
	s.e.NoError(err)
}

/*
func (s *S3TestSuite) TestS3_051_AddRemoteNoPassword() {
	res, _, err := s.remoteApi.CreateRemote(s.ctx, "foo", titanclient.Remote{
		Provider: "ssh",
		Name:     "origin",
		Properties: map[string]interface{}{
			"address":  s.e.S3Host,
			"username": "test",
			"port":     22,
			"path":     "/bar",
		},
	})
	if s.e.NoError(err) {
		s.Equal("origin", res.Name)
		s.Equal(s.e.S3Host, res.Properties["address"])
		s.Equal("test", res.Properties["username"])
		s.Nil(res.Properties["password"])
		s.Equal(22.0, res.Properties["port"])
		s.Equal("/bar", res.Properties["path"])
	}
}

func (s *S3TestSuite) TestS3_052_ListCommitsPassword() {
	res, _, err := s.remoteApi.ListRemoteCommits(s.ctx, "foo", "origin",
		titanclient.RemoteParameters{
			Provider:   "ssh",
			Properties: map[string]interface{}{"password": "test"},
		}, nil)
	if s.e.NoError(err) {
		s.Len(res, 1)
		s.Equal("id", res[0].Id)
	}
}

func (s *S3TestSuite) TestS3_053_ListCommitsNoPassword() {
	_, _, err := s.remoteApi.ListRemoteCommits(s.ctx, "foo", "origin", s.remoteParams, nil)
	s.e.APIError(err, "IllegalArgumentException")
}

func (s *S3TestSuite) TestS3_054_ListCommitsBadPassword() {
	_, _, err := s.remoteApi.ListRemoteCommits(s.ctx, "foo", "origin",
		titanclient.RemoteParameters{
			Provider:   "ssh",
			Properties: map[string]interface{}{"password": "r00t"},
		}, nil)
	s.e.APIError(err, "CommandException")
}

func (s *S3TestSuite) TestS3_060_CopyKey() {
	key, err := ioutil.ReadFile("id_rsa.pub")
	if s.e.NoError(err) {
		err = s.e.WriteFileSssh("/home/test/.ssh/authorized_keys", string(key))
		s.e.NoError(err)
	}
}

func (s *S3TestSuite) TestS3_061_ListCommitsKey() {
	key, err := ioutil.ReadFile("id_rsa")
	if s.e.NoError(err) {
		res, _, err := s.remoteApi.ListRemoteCommits(s.ctx, "foo", "origin", titanclient.RemoteParameters{
			Provider:   "ssh",
			Properties: map[string]interface{}{"key": string(key)},
		}, nil)
		if s.e.NoError(err) {
			s.Len(res, 1)
			s.Equal("id", res[0].Id)
		}
	}
}

func (s *S3TestSuite) TestS3_062_PullCommitKey() {
	key, err := ioutil.ReadFile("id_rsa")
	if s.e.NoError(err) {
		_, err := s.commitApi.DeleteCommit(s.ctx, "foo", "id")
		if s.e.NoError(err) {
			res, _, err := s.operationsApi.Pull(s.ctx, "foo", "origin", "id", titanclient.RemoteParameters{
				Provider:   "ssh",
				Properties: map[string]interface{}{"key": string(key)},
			}, nil)
			if s.e.NoError(err) {
				_, err = s.e.WaitForOperation(res.Id)
				s.e.NoError(err)
			}
		}
	}
}
*/

func (s *S3TestSuite) TestS3_070_DeleteVolume() {
	_, err := s.volumeApi.DeactivateVolume(s.ctx, "foo", "vol")
	if s.e.NoError(err) {
		_, err = s.volumeApi.DeleteVolume(s.ctx, "foo", "vol")
		s.e.NoError(err)
	}
}

func (s *S3TestSuite) TestS3_071_DeleteRepository() {
	_, err := s.repoApi.DeleteRepository(s.ctx, "foo")
	s.e.NoError(err)
}
