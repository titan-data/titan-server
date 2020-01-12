/*
 * Copyright The Titan Project Contributors
 */
package endtoend

import (
	"context"
	"errors"
	"fmt"
	titan "github.com/titan-data/titan-client-go"
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
		args = append(args, "-d", "--restart", "always", "--name", fmt.Sprintf("%s-launch", u.Identity),
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
		"run", "-d", "--restart", "always", "--name", fmt.Sprintf("%s-server", u.Identity),
		"-v", fmt.Sprintf("%s/.kube:/root/.kube", homeDir),
		"-v", fmt.Sprintf("%s-data:/var/lib/%s", u.Identity, u.Identity),
		"-e", "TITAN_CONTEXT=kubernetes-csi",
		"-e", fmt.Sprintf("TITAN_IDENTITY=%s", u.Identity),
		"-e", fmt.Sprintf("TITAN_CONFIG=%s", strings.Join(parameters, ",")),
		"-p", fmt.Sprintf("$s:5001", u.Port), u.Image, "/bin/bash",
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

func (u *dockerUtil) GetContainerName() string {
	var containerName string
	if u.Context == "docker-zfs" {
		containerName = "launch"
	} else {
		containerName = "sever"
	}
	return fmt.Sprintf("%s-%s", u.Identity, containerName)
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
				logs, err := exec.Command("docker", "logs", u.GetContainerName()).Output()
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
	return exec.Command("docker", "rm", "-f", u.GetContainerName()).Run()
}

func (u *dockerUtil) StopServer(ignoreErrors bool) error {
	if u.Context == "docker-zfs" {
		err := exec.Command("docker", "rm", "-f", fmt.Sprintf("%s-launch", u.Identity)).Run()
		if err != nil && !ignoreErrors {
			return err
		}
	}
	err := exec.Command("docker", "rm", "-f", fmt.Sprintf("%s-server", u.Identity)).Run()
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
		return nil, err
	}
	return v.Config["mountpoint"].(string), nil
}

func (u *dockerUtil) ExecServer(args ...string) (string, error) {
	fullArgs := []string{"exec", fmt.Sprintf("%s-server", u.Identity)}
	fullArgs = append(fullArgs, args...)
	out, err := exec.Command("docker", fullArgs...).Output()
	if err != nil {
		return "", err
	}
	return string(out), nil
}

/*

   fun writeFile(repo: String, volume: String, filename: String, content: String) {
       val mountpoint = getVolumePath(repo, volume)
       val path = "$mountpoint/$filename"
       // Using 'docker cp' can mess with volume mounts, leave this as simple as possible
       executor.exec("docker", "exec", "$identity-server", "sh", "-c",
               "echo \"$content\" > $path")
   }

   fun readFile(repo: String, volume: String, filename: String): String {
       val mountpoint = getVolumePath(repo, volume)
       val path = "$mountpoint/$filename"
       return executor.exec("docker", "exec", "$identity-server", "cat", path)
   }

   fun pathExists(path: String): Boolean {
       try {
           executor.exec("docker", "exec", "$identity-server", "ls", path)
       } catch (e: CommandException) {
           return false
       }
       return true
   }

   fun writeFileSsh(path: String, content: String) {
       executor.exec("docker", "exec", "$identity-ssh", "sh", "-c",
               "echo \"$content\" > $path")
   }

   fun readFileSsh(path: String): String {
       return executor.exec("docker", "exec", "$identity-ssh", "cat", path)
   }

   fun mkdirSsh(path: String) {
       executor.exec("docker", "exec", "$identity-ssh", "mkdir", "-p", path)
       executor.exec("docker", "exec", "$identity-ssh", "chown", sshUser, path)
   }

   fun startSsh() {
       executor.exec("docker", "run", "-p", "$sshPort:22", "-d", "--name", "$identity-ssh",
               "--network", "$identity", "titandata/ssh-test-server:latest")
   }

   fun testSsh(): Boolean {
       val jsch = JSch()
       try {
           val session = jsch.getSession(sshUser, "localhost", sshPort)
           session.setPassword(sshPassword)
           session.setConfig("StrictHostKeyChecking", "no")
           session.connect(timeout.toInt())
       } catch (e: Exception) {
           return false
       }
       return true
   }

   fun waitForSsh() {
       var tried = 1
       while (!testSsh()) {
           if (tried++ == retries) {
               throw Exception("Timed out waiting for SSH server to start")
           }
           Thread.sleep(timeout)
       }
   }

   fun stopSsh(ignoreExceptions: Boolean = true) {
       try {
           executor.exec("docker", "rm", "-f", "$identity-ssh")
       } catch (e: CommandException) {
           if (!ignoreExceptions) {
               throw e
           }
       }
   }

   fun getSshHost(): String {
       return executor.exec("docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
               "$identity-ssh").trim()
   }

   fun getSshUri(): String {
       // We explicitly add the port even though it's superfluous, as it helps validate serialization
       return "ssh://$sshUser:$sshPassword@${getSshHost()}:22"
   }
*/
