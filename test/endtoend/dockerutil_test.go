/*
 * Copyright The Titan Project Contributors
 */
package endtoend

import (
	"context"
	"errors"
	"fmt"
	titan "github.com/titan-data/titan-client-go"
	"golang.org/x/crypto/ssh"
	"os/exec"
	"os/user"
	"strings"
	"time"
)

/*
 * Utility class for managing docker containers for integration tests. There are two types of
 * containers we care about: the titan server container and a remote SSH container. The titan
 * server is run on an alternate pool and port so as not to conflict with the running titan-server.
 * For the remote SSH server, we use 'rastasheep/ubuntu-sshd', which comes pre-built for remote access
 * over SSH.
 */
type dockerUtil struct {
	Context  string
	Identity string
	Port     int
	Image    string
	SshPort  int

	Client *titan.APIClient
}

const waitTimeout = 1
const waitRetries = 60
const sshUser = "test"
const sshPassword = "test"

func DockerUtil(context string) *dockerUtil {
	ret := dockerUtil{
		Context:  context,
		Identity: "test",
		Port:     6001,
		Image:    "titan:latest",
		SshPort:  6003,
	}

	cfg := titan.NewConfiguration()
	cfg.Host = fmt.Sprintf("localhost:%d", ret.Port)
	ret.Client = titan.NewAPIClient(cfg)

	return &ret
}

func (u *dockerUtil) RunTitanDocker(entryPoint string, daemon bool) error {
	args := []string{"run", "--privileged", "--pid=host", "--network=host",
		"-v", "/var/lib:/var/lib", "-v", "/run/docker:/run/docker"}
	if daemon {
		args = append(args, "-d", "--restart", "always", "--name", u.GetPrimaryContainer(),
			"-v", fmt.Sprintf("/lib:/var/lib/%s/system", u.Identity))
	} else {
		args = append(args, "--rm")
	}
	args = append(args,
		"-v", fmt.Sprintf("%s-data:/var/lib/%s/data", u.Identity, u.Identity),
		"-v", "/var/run/docker.sock:/var/run/docker.sock",
		"-e", fmt.Sprintf("TITAN_IDENTITY=%s", u.Identity),
		"-e", fmt.Sprintf("TITAN_IMAGE=%s", u.Image),
		"-e", fmt.Sprintf("TITAN_PORT=%d", u.Port),
		u.Image, "/bin/bash", fmt.Sprintf("/titan/%s", entryPoint))

	return exec.Command("docker", args...).Run()
}

func (u *dockerUtil) RunTitanKubernetes(entryPoint string, parameters ...string) error {
	usr, err := user.Current()
	if err != nil {
		return err
	}
	homeDir := usr.HomeDir
	if homeDir == "" {
		return errors.New("failed to determine user home directory")
	}
	args := []string{
		"run", "-d", "--restart", "always", "--name", u.GetPrimaryContainer(),
		"-v", fmt.Sprintf("%s/.kube:/root/.kube", homeDir),
		"-v", fmt.Sprintf("%s-data:/var/lib/%s", u.Identity, u.Identity),
		"-e", "TITAN_CONTEXT=kubernetes-csi",
		"-e", fmt.Sprintf("TITAN_IDENTITY=%s", u.Identity),
		"-e", fmt.Sprintf("TITAN_CONFIG=%s", strings.Join(parameters, ",")),
		"-p", fmt.Sprintf("%d:5001", u.Port), u.Image, "/bin/bash",
		fmt.Sprintf("/titan/%s", entryPoint),
	}

	return exec.Command("docker", args...).Run()
}

func (u *dockerUtil) StartServer(parameters ...string) error {
	err := exec.Command("docker", "volume", "create", fmt.Sprintf("%s-data", u.Identity)).Run()
	if err != nil {
		return err
	}
	if u.Context == "docker-zfs" {
		return u.RunTitanDocker("launch", true)
	} else {
		return u.RunTitanKubernetes("run", parameters...)
	}
}

func (u *dockerUtil) GetContainer(t string) string {
	return fmt.Sprintf("%s-%s", u.Identity, t)
}

func (u *dockerUtil) GetPrimaryContainer() string {
	var containerType string
	if u.Context == "docker-zfs" {
		containerType = "launch"
	} else {
		containerType = "sever"
	}
	return u.GetContainer(containerType)
}

func (u *dockerUtil) WaitForServer() error {
	success := false
	tried := 1
	for ok := true; ok; ok = !success {
		_, _, err := u.Client.RepositoriesApi.ListRepositories(context.Background())
		if err == nil {
			success = true
		} else {
			tried++
			if tried == waitRetries {
				logs, err := exec.Command("docker", "logs", u.GetPrimaryContainer()).CombinedOutput()
				if err != nil {
					return err
				}
				return errors.New(fmt.Sprintf("timeoed out waiting for server to start: %s", logs))
			}
			time.Sleep(time.Duration(waitTimeout) * time.Second)
		}
	}
	return nil
}

func (u *dockerUtil) RestartServer() error {
	return exec.Command("docker", "rm", "-f", u.GetContainer("server")).Run()
}

func (u *dockerUtil) StopServer(ignoreErrors bool) error {
	if u.Context == "docker-zfs" {
		err := exec.Command("docker", "rm", "-f", u.GetContainer("launch")).Run()
		if err != nil && !ignoreErrors {
			return err
		}
	}
	err := exec.Command("docker", "rm", "-f", u.GetContainer("server")).Run()
	if err != nil && !ignoreErrors {
		return err
	}

	if u.Context == "docker-zfs" {
		err = u.RunTitanDocker("teardown", false)
		if err != nil && !ignoreErrors {
			return err
		}
	}

	err = exec.Command("docker", "volume", "rm", fmt.Sprintf("%s-data", u.Identity)).Run()
	if err != nil && !ignoreErrors {
		return err
	}

	return nil
}

func (u *dockerUtil) GetVolumePath(repo string, volume string) (string, error) {
	v, _, err := u.Client.VolumesApi.GetVolume(context.Background(), repo, volume)
	if err != nil {
		return "", err
	}
	return v.Config["mountpoint"].(string), nil
}

func (u *dockerUtil) ExecServer(args ...string) (string, error) {
	fullArgs := []string{"exec", u.GetContainer("server")}
	fullArgs = append(fullArgs, args...)
	out, err := exec.Command("docker", fullArgs...).Output()
	if err != nil {
		return "", err
	}
	return string(out), nil
}

func (u *dockerUtil) WriteFile(repo string, volume string, filename string, content string) error {
	mountpoint, err := u.GetVolumePath(repo, volume)
	if err != nil {
		return err
	}
	path := fmt.Sprintf("%s/%s", mountpoint, filename)
	return exec.Command("docker", "exec", u.GetContainer("server"), "sh", "-c",
		fmt.Sprintf("echo -n \"%s\" > %s", content, path)).Run()
}

func (u *dockerUtil) ReadFile(repo string, volume string, filename string) (string, error) {
	mountpoint, err := u.GetVolumePath(repo, volume)
	if err != nil {
		return "", err
	}
	path := fmt.Sprintf("%s/%s", mountpoint, filename)
	out, err := exec.Command("docker", "exec", u.GetContainer("server"), "cat", path).Output()
	if err != nil {
		return "", err
	}
	return string(out), nil
}

func (u *dockerUtil) PathExists(path string) bool {
	err := exec.Command("docker", "exec", u.GetContainer("server"), "ls", path).Run()
	return err != nil
}

func (u *dockerUtil) WriteFileSssh(path string, content string) error {
	return exec.Command("docker", "exec", u.GetContainer("ssh"), "sh", "-c",
		fmt.Sprintf("echo \"%s\" > %s", content, path)).Run()
}

func (u *dockerUtil) ReadFileSsh(path string) (string, error) {
	out, err := exec.Command("docker", "exec", u.GetContainer("ssh"), "cat", path).Output()
	if err != nil {
		return "", err
	}
	return string(out), nil
}

func (u *dockerUtil) MkdirSsh(path string) error {
	err := exec.Command("docker", "exec", u.GetContainer("ssh"), "mkdir", "-p", path).Run()
	if err != nil {
		return err
	}
	return exec.Command("docker", "exec", u.GetContainer("ssh"), "chown", sshUser, path).Run()
}

func (u *dockerUtil) StartSsh() error {
	return exec.Command("docker", "run", "-p", fmt.Sprintf("%d:22", u.Port), "-d", "--name", u.GetContainer("ssh"),
		"--network", u.Identity, "titandata/ssh-test-server:latest").Run()

}

func (u *dockerUtil) StopSsh() error {
	return exec.Command("docker", "rm", "-f", u.GetContainer("ssh")).Run()
}

func (u *dockerUtil) GetSshHost() (string, error) {
	out, err := exec.Command("docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
		u.GetContainer("ssh")).Output()
	if err != nil {
		return "", err
	}
	return string(out), nil
}

func (u *dockerUtil) GetSshUrl() (string, error) {
	host, err := u.GetSshHost()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("ssh://%s:%s@%s:22", sshUser, sshPassword, host), nil

}

func (u *dockerUtil) WaitForSsh() error {
	success := false
	tried := 1
	for ok := true; ok; ok = !success {
		sshConfig := &ssh.ClientConfig{
			User: sshUser,
			Auth: []ssh.AuthMethod{
				ssh.Password(sshPassword),
			},
		}
		connection, err := ssh.Dial("tcp", fmt.Sprintf("localhost:%d", u.SshPort), sshConfig)
		if err == nil {
			session, err := connection.NewSession()
			if err == nil {
				success = true
				_ = session.Close()
			}
			_ = connection.Close()
		}

		if !success {
			tried++
			if tried == waitRetries {
				logs, err := exec.Command("docker", "logs", u.GetPrimaryContainer()).CombinedOutput()
				if err != nil {
					return err
				}
				return errors.New(fmt.Sprintf("timeoed out waiting for server to start: %s", logs))
			}
			time.Sleep(time.Duration(waitTimeout) * time.Second)
		}
	}
	return nil
}
func (d *dockerUtil) SetupStandardDocker() {
	err := d.StopServer(true)
	if err != nil {
		panic(err)
	}
	err = d.StartServer()
	if err != nil {
		panic(err)
	}
	err = d.WaitForServer()
	if err != nil {
		panic(err)
	}
}

func (d *dockerUtil) TeardownStandardDocker() {
	err := d.StopServer(false)
	if err != nil {
		panic(err)
	}
}

func (d *dockerUtil) GetAPIError(err error) titan.ApiError {
	if openApiError, ok := err.(titan.GenericOpenAPIError); ok {
		if titanApiError, ok := openApiError.Model().(titan.ApiError); ok {
			return titanApiError
		}
	}
	return titan.ApiError{Message: err.Error()}
}

func (d *dockerUtil) GetTag(commit titan.Commit, tag string) string {
	if tags, ok := commit.Properties["tags"].(map[string]interface{}); ok {
		return tags[tag].(string)
	}
	return ""
}
