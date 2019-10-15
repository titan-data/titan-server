/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.remote.s3web

import com.google.gson.GsonBuilder
import io.titandata.ProviderModule
import io.titandata.exception.NoSuchObjectException
import io.titandata.models.Commit
import io.titandata.models.Operation
import io.titandata.models.ProgressEntry
import io.titandata.models.Remote
import io.titandata.models.RemoteParameters
import io.titandata.operation.OperationExecutor
import io.titandata.remote.BaseRemoteProvider
import io.titandata.serialization.ModelTypeAdapters
import java.io.File
import java.io.IOException
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response

/**
 * The S3 provider is a very simple provider for reading commits created by the S3 provider. It's primary purpose is to
 * make public demo data available without requiring people to have some kind of AWS credentials. It should not be
 * used as a general purpose remote. The URL can be any URL to the S3 bucket, even behind CloudFront, such as:
 *
 *      s3web://demo.titan-data.io/hello-world/postgres
 *
 * The main thing is that it expects to find the same layout as the S3 provider generates, including a "titan" file
 * at the root of the repository that has all the commit metadata.
 */
class S3WebRemoteProvider(val providers: ProviderModule) : BaseRemoteProvider() {

    private val gson = ModelTypeAdapters.configure(GsonBuilder()).create()
    private val client = OkHttpClient()

    fun getFile(remote: Remote, path: String): Response {
        remote as S3WebRemote
        val request = Request.Builder().url("${remote.url}/$path").build()
        return client.newCall(request).execute()
    }

    private fun getAllCommits(remote: Remote): List<Commit> {
        remote as S3WebRemote
        val response = getFile(remote, "titan")
        val body = when (response.isSuccessful) {
            true -> response.body!!.string()
            false -> when (response.code) {
                404 -> ""
                else -> throw IOException("failed to get ${remote.url}/titan, error code ${response.code}")
            }
        }

        val ret = mutableListOf<Commit>()

        for (line in body.split("\n")) {
            if (line != "") {
                ret.add(gson.fromJson(line, Commit::class.java))
            }
        }

        return ret
    }

    override fun listCommits(remote: Remote, params: RemoteParameters): List<Commit> {
        return getAllCommits(remote)
    }

    override fun getCommit(remote: Remote, commitId: String, params: RemoteParameters): Commit {
        val commits = getAllCommits(remote)
        val commit = commits.find { it.id == commitId }

        return commit
                ?: throw NoSuchObjectException("no such commit $commitId in remote '${remote.name}'")
    }

    // TODO refactor this into manageable chunks
    override fun runOperation(operation: OperationExecutor) {
        if (operation.operation.type == Operation.Type.PUSH) {
            throw IllegalStateException("push operations are not supported for s3web provider")
        }

        val repo = operation.repo
        val operationId = operation.operation.id
        val remote = operation.remote as S3WebRemote
        val commitId = operation.operation.commitId

        val scratch = providers.storage.createOperationScratch(repo, operationId)
        try {
            val base = providers.storage.mountOperationVolumes(repo, operationId)
            try {
                for (vol in providers.storage.listVolumes(repo)) {
                    val desc = vol.properties?.get("path")?.toString() ?: vol.name

                    operation.addProgress(ProgressEntry(type = ProgressEntry.Type.START,
                            message = "Downloading archive for $desc"))

                    val path = "$commitId/${vol.name}.tar.gz"
                    val response = getFile(remote, path)
                    if (!response.isSuccessful) {
                        throw IOException("failed to get ${remote.url}/$path, error code ${response.code}")
                    }
                    val archive = "$scratch/${vol.name}.tar.gz"
                    val archiveFile = File(archive)
                    response.body!!.byteStream().use { input ->
                        archiveFile.outputStream().use { output ->
                            input.copyTo(output)
                        }
                    }

                    operation.addProgress(ProgressEntry(type = ProgressEntry.Type.END))

                    operation.addProgress(ProgressEntry(type = ProgressEntry.Type.START,
                            message = "Extracting archive for $desc"))
                    val args = arrayOf("tar", "xzf", archive)
                    val process = ProcessBuilder()
                            .directory(File("$base/${vol.name}"))
                            .command(*args)
                            .start()
                    providers.commandExecutor.exec(process, args.joinToString())
                    operation.addProgress(ProgressEntry(type = ProgressEntry.Type.END))
                }
            } finally {
                providers.storage.unmountOperationVolumes(repo, operationId)
            }
        } finally {
            providers.storage.destroyOperationScratch(repo, operationId)
        }
        // TODO cleanup on failure
    }
}
