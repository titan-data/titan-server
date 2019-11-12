/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.remote.ssh

import com.google.gson.GsonBuilder
import io.titandata.ProviderModule
import io.titandata.exception.CommandException
import io.titandata.exception.NoSuchObjectException
import io.titandata.models.Commit
import io.titandata.models.Operation
import io.titandata.models.ProgressEntry
import io.titandata.models.Remote
import io.titandata.models.RemoteParameters
import io.titandata.models.Volume
import io.titandata.operation.OperationExecutor
import io.titandata.remote.BaseRemoteProvider
import io.titandata.sync.RsyncExecutor
import io.titandata.util.TagFilter
import java.io.File
import java.io.IOException
import java.nio.file.Files
import java.nio.file.attribute.PosixFilePermission
import java.util.concurrent.TimeUnit

/**
 * The SSH remote provide is responsible for pushing and pulling data from an arbitrary SSH host.
 * This is a baseline provider to showcase what is possible, and is not designed to be particularly
 * efficient or robust in the face of failure.
 *
 * When you add a remote, you specify the path where you want the committed data to be stored. This
 * is a flat directory, where each sub-directory is a commit. Within each commit, you have a
 * metadata.json file, as well as a 'data' directory that then contains data for each volume. The
 * result looks like something like this:
 *
 *      /path
 *          /<commitId>
 *              /metadata.json
 *              /data
 *                  /<volumeName>
 *
 * This is obviously not particularly scalable, and each push/pull requires a full data transfer.
 * But it's very simple, and can be deployed very easily.
 *
 * The data transfer itself is done via rsync, the details of which are explained in the
 * RsyncExecutor class.
 *
 * One challenge is using password-based authentication with SSH. While we may encourage the use of
 * SSH keys, for simple use cases we want to allow the user to supply a password, either as part
 * of the remote (discouraged, since it'll be stored forever on disk) or at the time of the
 * push/pull operation (better, since it'll only be stored for the duration of the operation).
 * SSH is very careful about detecting terminals and forcing passwords to be provided interactively,
 * so we use the sshpass utility to be able to pass thosec credentials.
 */
class SshRemoteProvider(val providers: ProviderModule) : BaseRemoteProvider() {

    internal val gson = GsonBuilder().create()

    fun getSshAuth(remote: Remote, params: RemoteParameters): Pair<String?, String?> {
        if (params.properties["password"] != null && params.properties["key"] != null) {
            throw IllegalArgumentException("only one of password or key can be specified")
        } else if (remote.properties["password"] != null || params.properties["password"] != null) {
            return Pair((params.properties["password"] ?: remote.properties["password"]).toString(), null)
        } else if (params.properties["key"] != null) {
            return Pair(null, params.properties["key"].toString())
        } else {
            throw IllegalArgumentException("one of password or key must be specified")
        }
    }

    fun buildSshCommand(
        remote: Remote,
        params: RemoteParameters,
        file: File,
        includeAddress: Boolean,
        vararg command: String
    ): List<String> {
        val args = mutableListOf<String>()

        val (password, key) = getSshAuth(remote, params)

        if (password != null) {
            file.writeText(password)
            args.addAll(arrayOf("sshpass", "-f", file.path, "ssh"))
        } else if (key != null) {
            file.writeText(key)
            args.addAll(arrayOf("ssh", "-i", file.path))
        } else {
            throw IllegalArgumentException("one of password or key must be specified")
        }
        Files.setPosixFilePermissions(file.toPath(), mutableSetOf(
                PosixFilePermission.OWNER_READ
        ))

        val props = remote.properties
        if (props["port"] != null) {
            args.addAll(arrayOf("-p", props["port"].toString()))
        }

        args.addAll(arrayOf("-o", "StrictHostKeyChecking=no"))
        args.addAll(arrayOf("-o", "UserKnownHostsFile=/dev/null"))
        if (includeAddress) {
            args.add("${props["username"]}@${props["address"]}")
        }
        args.addAll(command)

        return args
    }

    private fun runSsh(remote: Remote, params: RemoteParameters, vararg command: String): String {

        val file = createTempFile()
        file.deleteOnExit()
        try {
            val args = buildSshCommand(remote, params, file, true, *command)
            return providers.commandExecutor.exec(*args.toTypedArray())
        } finally {
            file.delete()
        }
    }

    private fun writeFileSsh(remote: Remote, params: RemoteParameters, path: String, content: String) {
        val file = createTempFile()
        file.deleteOnExit()
        try {
            val args = buildSshCommand(remote, params, file, true, "sh", "-c", "cat > $path")
            val process = providers.commandExecutor.start(*args.toTypedArray())
            val writer = process.outputStream.bufferedWriter()
            writer.write(content)
            writer.close()
            process.outputStream.close()
            process.waitFor(10L, TimeUnit.SECONDS)

            if (process.isAlive) {
                throw IOException("Timed out waiting for command: $args")
            }
            providers.commandExecutor.checkResult(process)
        } finally {
            file.delete()
        }
    }

    override fun listCommits(remote: Remote, params: RemoteParameters, tags: List<String>?): List<Commit> {
        val output = runSsh(remote, params, "ls", "-1", remote.properties["path"] as String)
        val commits = mutableListOf<Commit>()
        val filter = TagFilter(tags)
        for (line in output.lines()) {
            val commitId = line.trim()
            if (commitId != "") {
                try {
                    val commit = getCommit(remote, commitId, params)
                    if (filter.match(commit)) {
                        commits.add(commit)
                    }
                } catch (e: NoSuchObjectException) {
                    // Ignore broken links
                }
            }
        }

        return commits
    }

    override fun getCommit(remote: Remote, commitId: String, params: RemoteParameters): Commit {
        try {
            val json = runSsh(remote, params, "cat", "${remote.properties["path"]}/$commitId/metadata.json")
            return gson.fromJson(json, Commit::class.java)
        } catch (e: CommandException) {
            if (e.output.contains("No such file or directory")) {
                throw NoSuchObjectException("no such commit $commitId in remote '${remote.name}'")
            }
            throw e
        }
    }

    override fun syncVolume(operation: OperationExecutor, data: Any?, volume: Volume, path: String, scratchPath: String) {
        val localPath = "$path/"
        val props = operation.remote.properties
        val remoteDir = "${props["path"]}/${operation.operation.commitId}/data/${volume.name}"
        val remotePath = "${props["username"]}@${props["address"]}:$remoteDir/"
        val src = when (operation.operation.type) {
            Operation.Type.PUSH -> localPath
            Operation.Type.PULL -> remotePath
        }
        val dst = when (operation.operation.type) {
            Operation.Type.PUSH -> remotePath
            Operation.Type.PULL -> localPath
        }

        val desc = getVolumeDesc(volume)
        operation.addProgress(ProgressEntry(type = ProgressEntry.Type.START,
                message = "Syncing $desc", percent = 0))

        if (operation.operation.type == Operation.Type.PUSH) {
            runSsh(operation.remote, operation.params, "sudo", "mkdir", "-p", remoteDir)
        }

        val (password, key) = getSshAuth(operation.remote, operation.params)
        val rsync = RsyncExecutor(operation, operation.remote.properties["port"] as Int?, password,
                key, "$src/", dst, providers.commandExecutor)
        rsync.run()
    }

    override fun pushMetadata(operation: OperationExecutor, data: Any?, commit: Commit, isUpdate: Boolean) {
        val json = gson.toJson(commit)
        writeFileSsh(operation.remote, operation.params,
                "${operation.remote.properties["path"]}/${operation.operation.commitId}/metadata.json", json)
    }
}
