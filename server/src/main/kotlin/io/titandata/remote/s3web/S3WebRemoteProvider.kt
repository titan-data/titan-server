/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.remote.s3web

import com.google.gson.GsonBuilder
import io.titandata.ProviderModule
import io.titandata.exception.NoSuchObjectException
import io.titandata.models.Commit
import io.titandata.models.ProgressEntry
import io.titandata.models.Remote
import io.titandata.models.RemoteParameters
import io.titandata.models.Volume
import io.titandata.operation.OperationExecutor
import io.titandata.remote.BaseRemoteProvider
import io.titandata.util.TagFilter
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

    private val gson = GsonBuilder().create()
    private val client = OkHttpClient()

    fun getFile(remote: Remote, path: String): Response {
        val url = remote.properties["url"] as String
        val request = Request.Builder().url("$url/$path").build()
        return client.newCall(request).execute()
    }

    override fun pushVolume(operation: OperationExecutor, data: Any?, volume: Volume, path: String, scratchPath: String) {
        throw IllegalStateException("push operations are not supported for s3web provider")
    }

    override fun pullVolume(operation: OperationExecutor, data: Any?, volume: Volume, path: String, scratchPath: String) {
        val desc = getVolumeDesc(volume)
        val commitId = operation.operation.commitId
        val url = operation.remote.properties["url"] as String

        operation.addProgress(ProgressEntry(type = ProgressEntry.Type.START,
                message = "Downloading archive for $desc"))

        val archivePath = "$commitId/${volume.name}.tar.gz"
        val response = getFile(operation.remote, archivePath)
        if (!response.isSuccessful) {
            throw IOException("failed to get $url/$archivePath, error code ${response.code}")
        }
        val archive = "$scratchPath/${volume.name}.tar.gz"
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
                .directory(File(path))
                .command(*args)
                .start()
        providers.commandExecutor.exec(process, args.joinToString())
        operation.addProgress(ProgressEntry(type = ProgressEntry.Type.END))
    }

    override fun pushMetadata(operation: OperationExecutor, data: Any?, commit: Commit, isUpdate: Boolean) {
        throw IllegalStateException("push operations are not supported for s3web provider")
    }
}
