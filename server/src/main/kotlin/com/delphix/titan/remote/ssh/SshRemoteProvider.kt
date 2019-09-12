/*
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */

package com.delphix.titan.remote.ssh

import com.delphix.titan.ProviderModule
import com.delphix.titan.exception.CommandException
import com.delphix.titan.exception.NoSuchObjectException
import com.delphix.titan.models.Commit
import com.delphix.titan.models.Operation
import com.delphix.titan.models.ProgressEntry
import com.delphix.titan.models.Remote
import com.delphix.titan.models.RemoteParameters
import com.delphix.titan.models.SshParameters
import com.delphix.titan.models.SshRemote
import com.delphix.titan.operation.OperationExecutor
import com.delphix.titan.remote.BaseRemoteProvider
import com.delphix.titan.serialization.ModelTypeAdapters
import com.delphix.titan.sync.RsyncExecutor
import com.google.gson.GsonBuilder
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

    internal val gson = ModelTypeAdapters.configure(GsonBuilder()).create()

    fun getSshAuth(remote: Remote, params: RemoteParameters): Pair<String?, String?> {
        val sshRemote = remote as SshRemote
        val sshParams = params as SshParameters

        if (sshParams.password != null && sshParams.key != null) {
            throw IllegalArgumentException("only one of password or key can be specified")
        } else if (sshRemote.password != null || sshParams.password != null) {
            return Pair(sshParams.password ?: sshRemote.password, null)
        } else if (sshParams.key != null) {
            return Pair(null, sshParams.key)
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
        remote as SshRemote
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

        if (remote.port != null) {
            args.addAll(arrayOf("-p", remote.port.toString()))
        }

        args.addAll(arrayOf("-o", "StrictHostKeyChecking=no"))
        if (includeAddress) {
            args.add("${remote.username}@${remote.address}")
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

    override fun listCommits(remote: Remote, params: RemoteParameters): List<Commit> {
        val sshRemote = remote as SshRemote

        val output = runSsh(remote, params, "ls", "-1", sshRemote.path)
        val commits = mutableListOf<Commit>()
        for (line in output.lines()) {
            val commitId = line.trim()
            if (commitId != "") {
                try {
                    commits.add(getCommit(remote, commitId, params))
                } catch (e: NoSuchObjectException) {
                    // Ignore broken links
                }
            }
        }

        return commits
    }

    override fun getCommit(remote: Remote, commitId: String, params: RemoteParameters): Commit {
        val sshRemote = remote as SshRemote

        try {
            val json = runSsh(remote, params, "cat", "${sshRemote.path}/$commitId/metadata.json")
            return gson.fromJson(json, Commit::class.java)
        } catch (e: CommandException) {
            if (e.output.contains("No such file or directory")) {
                throw NoSuchObjectException("no such commit $commitId in remote '${remote.name}'")
            }
            throw e
        }
    }

    override fun runOperation(operation: OperationExecutor) {
        val repo = operation.repo
        val operationId = operation.operation.id
        val remote = operation.remote as SshRemote
        val params = operation.params

        val base = providers.storage.mountOperationVolumes(repo, operationId)
        try {
            for (vol in providers.storage.listVolumes(repo)) {
                val desc = vol.properties?.get("path")?.toString() ?: vol.name
                val localPath = "$base/${vol.name}/"
                val remoteDir = "${remote.path}/${operation.operation.commitId}/data/${vol.name}"
                val remotePath = "${remote.username}@${remote.address}:$remoteDir/"
                val src = when (operation.operation.type) {
                    Operation.Type.PUSH -> localPath
                    Operation.Type.PULL -> remotePath
                }
                val dst = when (operation.operation.type) {
                    Operation.Type.PUSH -> remotePath
                    Operation.Type.PULL -> localPath
                }

                operation.addProgress(ProgressEntry(type = ProgressEntry.Type.START,
                        message = "Syncing $desc", percent = 0))

                if (operation.operation.type == Operation.Type.PUSH) {
                    runSsh(remote, params, "sudo", "mkdir", "-p", remoteDir)
                }

                val (password, key) = getSshAuth(remote, params)
                val rsync = RsyncExecutor(operation, remote.port, password,
                        key, "$src/", dst, providers.commandExecutor)
                rsync.run()
            }

            if (operation.operation.type == Operation.Type.PUSH) {
                val commit = providers.storage.getCommit(repo, operation.operation.commitId)
                val json = gson.toJson(commit)
                writeFileSsh(remote, params,
                        "${remote.path}/${operation.operation.commitId}/metadata.json", json)
            }
        } finally {
            providers.storage.unmountOperationVolumes(repo, operationId)
        }
    }
}
