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
	titan "github.com/titan-data/titan-client-go"
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
	s3remote      titan.Remote
	webRemote     titan.Remote
	s3parameters  titan.RemoteParameters
	webParameters titan.RemoteParameters
	currentOp     titan.Operation
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

	s.s3remote = titan.Remote{
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
	s.webRemote = titan.Remote{
		Provider: "s3web",
		Name:     "web",
		Properties: map[string]interface{}{
			"url": fmt.Sprintf("http://%s.s3.amazonaws.com/%s", s.s3bucket, s.s3path),
		},
	}

	s.e = NewEndToEndTest(&s.Suite, "docker-zfs")
	s.e.SetupStandardDocker()

	s.ctx = context.Background()

	s.s3parameters = titan.RemoteParameters{
		Provider:   "s3",
		Properties: map[string]interface{}{},
	}

	s.webParameters = titan.RemoteParameters{
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
	_, _, err := s.e.RepoApi.CreateRepository(s.ctx, titan.Repository{
		Name:       "foo",
		Properties: map[string]interface{}{},
	})
	s.e.NoError(err)
}

func (s *S3WebTestSuite) TestS3Web_002_CreateMountVolume() {
	_, _, err := s.e.VolumeApi.CreateVolume(s.ctx, "foo", titan.Volume{
		Name:       "vol",
		Properties: map[string]interface{}{},
	})
	if s.e.NoError(err) {
		_, err := s.e.VolumeApi.ActivateVolume(s.ctx, "foo", "vol")
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
	res, _, err := s.e.CommitApi.CreateCommit(s.ctx, "foo", titan.Commit{
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
	_, _, err := s.e.RemoteApi.CreateRemote(s.ctx, "foo", s.s3remote)
	s.e.NoError(err)
}

func (s *S3WebTestSuite) TestS3Web_006_AddS3WebRemote() {
	_, _, err := s.e.RemoteApi.CreateRemote(s.ctx, "foo", s.webRemote)
	s.e.NoError(err)
}

func (s *S3WebTestSuite) TestS3Web_010_ListEmptyRemoteCommits() {
	res, _, err := s.e.RemoteApi.ListRemoteCommits(s.ctx, "foo", "web", s.webParameters, nil)
	if s.e.NoError(err) {
		s.Len(res, 0)
	}
}

func (s *S3WebTestSuite) TestS3Web_011_GetBadRemoteCommit() {
	_, _, err := s.e.RemoteApi.GetRemoteCommit(s.ctx, "foo", "web", "id2", s.webParameters)
	s.e.APIError(err, "NoSuchObjectException")
}

func (s *S3WebTestSuite) TestS3Web_020_PushCommit() {
	res, _, err := s.e.OperationsApi.Push(s.ctx, "foo", "origin", "id", s.s3parameters, nil)
	if s.e.NoError(err) {
		_, err = s.e.WaitForOperation(res.Id)
		s.e.NoError(err)
	}
}

func (s *S3WebTestSuite) TestS3Web_021_ListRemoteCommit() {
	res, _, err := s.e.RemoteApi.ListRemoteCommits(s.ctx, "foo", "web", s.webParameters, nil)
	if s.e.NoError(err) {
		s.Len(res, 1)
		s.Equal("id", res[0].Id)
		s.Equal("b", s.e.GetTag(res[0], "a"))
	}
}

func (s *S3WebTestSuite) TestS3Web_022_ListRemoteFilterOut() {
	res, _, err := s.e.RemoteApi.ListRemoteCommits(s.ctx, "foo", "web", s.webParameters,
		&titan.ListRemoteCommitsOpts{Tag: optional.NewInterface([]string{"e"})})
	if s.e.NoError(err) {
		s.Len(res, 0)
	}
}

func (s *S3WebTestSuite) TestS3Web_023_ListRemoteFilterInclude() {
	res, _, err := s.e.RemoteApi.ListRemoteCommits(s.ctx, "foo", "web", s.webParameters,
		&titan.ListRemoteCommitsOpts{Tag: optional.NewInterface([]string{"a=b", "c=d"})})
	if s.e.NoError(err) {
		s.Len(res, 1)
		s.Equal("id", res[0].Id)
	}
}

func (s *S3WebTestSuite) TestS3Web_030_CreateSecondCommit() {
	_, _, err := s.e.CommitApi.CreateCommit(s.ctx, "foo", titan.Commit{
		Id:         "id2",
		Properties: map[string]interface{}{},
	})
	s.e.NoError(err)
}

func (s *S3WebTestSuite) TestS3Web_031_PushWeb() {
	res, _, err := s.e.OperationsApi.Push(s.ctx, "foo", "web", "id2", s.webParameters, nil)
	if s.e.NoError(err) {
		progress, err := s.e.WaitForOperation(res.Id)
		s.Error(err)
		s.Equal("FAILED", progress[len(progress)-1].Type)
	}
}

func (s *S3WebTestSuite) TestS3Web_032_PushSecondCommit() {
	res, _, err := s.e.OperationsApi.Push(s.ctx, "foo", "origin", "id2", s.s3parameters, nil)
	if s.e.NoError(err) {
		_, err = s.e.WaitForOperation(res.Id)
		s.e.NoError(err)
	}
}

func (s *S3WebTestSuite) TestS3Web_033_ListMultipleCommits() {
	res, _, err := s.e.RemoteApi.ListRemoteCommits(s.ctx, "foo", "web", s.webParameters, nil)
	if s.e.NoError(err) {
		s.Len(res, 2)
		s.Equal("id2", res[0].Id)
		s.Equal("id", res[1].Id)
	}
}

func (s *S3WebTestSuite) TestS3Web_040_DeleteLocalCommits() {
	_, err := s.e.CommitApi.DeleteCommit(s.ctx, "foo", "id")
	if s.e.NoError(err) {
		_, err = s.e.CommitApi.DeleteCommit(s.ctx, "foo", "id2")
		s.e.NoError(err)
	}
}

func (s *S3WebTestSuite) TestS3Web_041_ListEmptyCommits() {
	res, _, err := s.e.CommitApi.ListCommits(s.ctx, "foo", nil)
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
	res, _, err := s.e.OperationsApi.Pull(s.ctx, "foo", "web", "id", s.webParameters, nil)
	if s.e.NoError(err) {
		_, err = s.e.WaitForOperation(res.Id)
		s.e.NoError(err)
	}
}

func (s *S3WebTestSuite) TestS3Web_044_PullDuplicate() {
	_, _, err := s.e.OperationsApi.Pull(s.ctx, "foo", "web", "id", s.webParameters, nil)
	s.e.APIError(err, "ObjectExistsException")
}

func (s *S3WebTestSuite) TestS3Web_046_CheckoutCommit() {
	_, err := s.e.VolumeApi.DeactivateVolume(s.ctx, "foo", "vol")
	if s.e.NoError(err) {
		_, err := s.e.CommitApi.CheckoutCommit(s.ctx, "foo", "id")
		if s.e.NoError(err) {
			_, err = s.e.VolumeApi.ActivateVolume(s.ctx, "foo", "vol")
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
	_, err := s.e.RemoteApi.DeleteRemote(s.ctx, "foo", "origin")
	s.e.NoError(err)
}

func (s *S3WebTestSuite) TestS3Web_051_DeleteVolume() {
	_, err := s.e.VolumeApi.DeactivateVolume(s.ctx, "foo", "vol")
	if s.e.NoError(err) {
		_, err = s.e.VolumeApi.DeleteVolume(s.ctx, "foo", "vol")
		s.e.NoError(err)
	}
}

func (s *S3WebTestSuite) TestS3Web_052_DeleteRepository() {
	_, err := s.e.RepoApi.DeleteRepository(s.ctx, "foo")
	s.e.NoError(err)
}
