/*
 * Copyright The Titan Project Contributors.
 */
package endtoend

import (
	"context"
	"fmt"
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

type S3WebTestSuite struct {
	suite.Suite
	e   *EndToEndTest
	ctx context.Context

	s3bucket      string
	s3path        string
	s3remote      titanclient.Remote
	webRemote     titanclient.Remote
	s3parameters  titanclient.RemoteParameters
	webParameters titanclient.RemoteParameters
	currentOp     titanclient.Operation

	repoApi       *titanclient.RepositoriesApiService
	remoteApi     *titanclient.RemotesApiService
	volumeApi     *titanclient.VolumesApiService
	commitApi     *titanclient.CommitsApiService
	operationsApi *titanclient.OperationsApiService
}

func (s *S3WebTestSuite) ClearBucket() error {
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

func (s *S3WebTestSuite) SetupSuite() {
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

	s.s3remote = titanclient.Remote{
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
	s.webRemote = titanclient.Remote{
		Provider: "s3web",
		Name:     "web",
		Properties: map[string]interface{}{
			"url": fmt.Sprintf("http://%s.s3.amazonaws.com/%s", s.s3bucket, s.s3path),
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

	s.s3parameters = titanclient.RemoteParameters{
		Provider:   "s3",
		Properties: map[string]interface{}{},
	}

	s.webParameters = titanclient.RemoteParameters{
		Provider:   "s3web",
		Properties: map[string]interface{}{},
	}
}

func (s *S3WebTestSuite) TearDownSuite() {
	s.e.TeardownStandardDocker()
}

func TestS3WebTestSuite(t *testing.T) {
	suite.Run(t, new(S3WebTestSuite))
}

func (s *S3WebTestSuite) TestS3Web_001_CreateRepository() {
	_, _, err := s.repoApi.CreateRepository(s.ctx, titanclient.Repository{
		Name:       "foo",
		Properties: map[string]interface{}{},
	})
	s.e.NoError(err)
}

func (s *S3WebTestSuite) TestS3Web_002_CreateMountVolume() {
	_, _, err := s.volumeApi.CreateVolume(s.ctx, "foo", titanclient.Volume{
		Name:       "vol",
		Properties: map[string]interface{}{},
	})
	if s.e.NoError(err) {
		_, err := s.volumeApi.ActivateVolume(s.ctx, "foo", "vol")
		s.e.NoError(err)
	}
}

func (s *S3WebTestSuite) TestS3Web_003_CreateFile() {
	err := s.e.WriteFile("foo", "vol", "testfile", "Hello")
	if s.e.NoError(err) {
		res, err := s.e.ReadFile("foo", "vol", "testfile")
		if s.e.NoError(err) {
			s.Equal("Hello", res)
		}
	}
}

func (s *S3WebTestSuite) TestS3Web_004_CreateCommit() {
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

func (s *S3WebTestSuite) TestS3Web_005_AddS3Remote() {
	_, _, err := s.remoteApi.CreateRemote(s.ctx, "foo", s.s3remote)
	s.e.NoError(err)
}

func (s *S3WebTestSuite) TestS3Web_006_AddS3WebRemote() {
	_, _, err := s.remoteApi.CreateRemote(s.ctx, "foo", s.webRemote)
	s.e.NoError(err)
}

func (s *S3WebTestSuite) TestS3Web_010_ListEmptyRemoteCommits() {
	res, _, err := s.remoteApi.ListRemoteCommits(s.ctx, "foo", "web", s.webParameters, nil)
	if s.e.NoError(err) {
		s.Len(res, 0)
	}
}

func (s *S3WebTestSuite) TestS3Web_011_GetBadRemoteCommit() {
	_, _, err := s.remoteApi.GetRemoteCommit(s.ctx, "foo", "web", "id2", s.webParameters)
	s.e.APIError(err, "NoSuchObjectException")
}

func (s *S3WebTestSuite) TestS3Web_020_PushCommit() {
	res, _, err := s.operationsApi.Push(s.ctx, "foo", "origin", "id", s.s3parameters, nil)
	if s.e.NoError(err) {
		_, err = s.e.WaitForOperation(res.Id)
		s.e.NoError(err)
	}
}

func (s *S3WebTestSuite) TestS3Web_021_ListRemoteCommit() {
	res, _, err := s.remoteApi.ListRemoteCommits(s.ctx, "foo", "web", s.webParameters, nil)
	if s.e.NoError(err) {
		s.Len(res, 1)
		s.Equal("id", res[0].Id)
		s.Equal("b", s.e.GetTag(res[0], "a"))
	}
}

func (s *S3WebTestSuite) TestS3Web_022_ListRemoteFilterOut() {
	res, _, err := s.remoteApi.ListRemoteCommits(s.ctx, "foo", "web", s.webParameters,
		&titanclient.ListRemoteCommitsOpts{Tag: optional.NewInterface([]string{"e"})})
	if s.e.NoError(err) {
		s.Len(res, 0)
	}
}

func (s *S3WebTestSuite) TestS3Web_023_ListRemoteFilterInclude() {
	res, _, err := s.remoteApi.ListRemoteCommits(s.ctx, "foo", "web", s.webParameters,
		&titanclient.ListRemoteCommitsOpts{Tag: optional.NewInterface([]string{"a=b", "c=d"})})
	if s.e.NoError(err) {
		s.Len(res, 1)
		s.Equal("id", res[0].Id)
	}
}

func (s *S3WebTestSuite) TestS3Web_030_CreateSecondCommit() {
	_, _, err := s.commitApi.CreateCommit(s.ctx, "foo", titanclient.Commit{
		Id:         "id2",
		Properties: map[string]interface{}{},
	})
	s.e.NoError(err)
}

func (s *S3WebTestSuite) TestS3Web_031_PushWeb() {
	res, _, err := s.operationsApi.Push(s.ctx, "foo", "web", "id2", s.webParameters, nil)
	if s.e.NoError(err) {
		progress, err := s.e.WaitForOperation(res.Id)
		s.Error(err)
		s.Equal("FAILED", progress[len(progress)-1].Type)
	}
}

func (s *S3WebTestSuite) TestS3Web_032_PushSecondCommit() {
	res, _, err := s.operationsApi.Push(s.ctx, "foo", "origin", "id2", s.s3parameters, nil)
	if s.e.NoError(err) {
		_, err = s.e.WaitForOperation(res.Id)
		s.e.NoError(err)
	}
}

func (s *S3WebTestSuite) TestS3Web_033_ListMultipleCommits() {
	res, _, err := s.remoteApi.ListRemoteCommits(s.ctx, "foo", "web", s.webParameters, nil)
	if s.e.NoError(err) {
		s.Len(res, 2)
		s.Equal("id2", res[0].Id)
		s.Equal("id", res[1].Id)
	}
}

func (s *S3WebTestSuite) TestS3Web_040_DeleteLocalCommits() {
	_, err := s.commitApi.DeleteCommit(s.ctx, "foo", "id")
	if s.e.NoError(err) {
		_, err = s.commitApi.DeleteCommit(s.ctx, "foo", "id2")
		s.e.NoError(err)
	}
}

func (s *S3WebTestSuite) TestS3Web_041_ListEmptyCommits() {
	res, _, err := s.commitApi.ListCommits(s.ctx, "foo", nil)
	if s.e.NoError(err) {
		s.Len(res, 0)
	}
}

func (s *S3WebTestSuite) TestS3Web_042_UpdateFile() {
	err := s.e.WriteFile("foo", "vol", "testfile", "Goodbye")
	if s.e.NoError(err) {
		res, err := s.e.ReadFile("foo", "vol", "testfile")
		if s.e.NoError(err) {
			s.Equal("Goodbye", res)
		}
	}
}

func (s *S3WebTestSuite) TestS3Web_043_PullCommit() {
	res, _, err := s.operationsApi.Pull(s.ctx, "foo", "web", "id", s.webParameters, nil)
	if s.e.NoError(err) {
		_, err = s.e.WaitForOperation(res.Id)
		s.e.NoError(err)
	}
}

func (s *S3WebTestSuite) TestS3Web_044_PullDuplicate() {
	_, _, err := s.operationsApi.Pull(s.ctx, "foo", "web", "id", s.webParameters, nil)
	s.e.APIError(err, "ObjectExistsException")
}

func (s *S3WebTestSuite) TestS3Web_046_CheckoutCommit() {
	_, err := s.volumeApi.DeactivateVolume(s.ctx, "foo", "vol")
	if s.e.NoError(err) {
		_, err := s.commitApi.CheckoutCommit(s.ctx, "foo", "id")
		if s.e.NoError(err) {
			_, err = s.volumeApi.ActivateVolume(s.ctx, "foo", "vol")
			s.e.NoError(err)
		}
	}
}

func (s *S3WebTestSuite) TestS3Web_047_OriginalContents() {
	res, err := s.e.ReadFile("foo", "vol", "testfile")
	if s.e.NoError(err) {
		s.Equal("Hello", res)
	}
}

func (s *S3WebTestSuite) TestS3Web_050_RemoveRemote() {
	_, err := s.remoteApi.DeleteRemote(s.ctx, "foo", "origin")
	s.e.NoError(err)
}

func (s *S3WebTestSuite) TestS3Web_051_DeleteVolume() {
	_, err := s.volumeApi.DeactivateVolume(s.ctx, "foo", "vol")
	if s.e.NoError(err) {
		_, err = s.volumeApi.DeleteVolume(s.ctx, "foo", "vol")
		s.e.NoError(err)
	}
}

func (s *S3WebTestSuite) TestS3Web_052_DeleteRepository() {
	_, err := s.repoApi.DeleteRepository(s.ctx, "foo")
	s.e.NoError(err)
}
