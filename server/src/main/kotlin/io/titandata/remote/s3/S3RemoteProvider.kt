/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.remote.s3

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.auth.BasicSessionCredentials
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.AmazonS3Exception
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest
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
import io.titandata.util.TagFilter
import java.io.ByteArrayInputStream
import java.io.File
import java.io.InputStream
import java.io.SequenceInputStream
import java.lang.IllegalArgumentException

/**
 * The S3 provider is a very simple provider for storing whole commits directly in a S3 bucket. Each commit is is a
 * key within a folder, for example:
 *
 *      s3://bucket/path/to/repo/3583-4053-598ea-298fa
 *
 * Within each commit sub-directory, there is .tar.gz file for each volume. The metadata for each commit is stored
 * as metadata for the object, as well in a 'titan' file at the root of the repository, with once line per commit. We
 * do this for a few reasons:
 *
 *      * Storing it in object metdata is inefficient, as there's no way to fetch the metadata of multiple objects
 *        at once. We keep it per-object for the cases where we
 *      * We want to be able to access this data in a read-only fashion over the HTTP interface, and there is no way
 *        to access object metadata (or even necessarily iterate over objects) through the HTTP interface.
 *
 * This has its downsides, namely that deleting a commit is more complicated, and there is greater risk of
 * concurrent operations creating invalid state, but those are existing challenges with these simplistic providers.
 * Properly solving them would require a more sophisticated provider with server-side logic.
 */
class S3RemoteProvider(val providers: ProviderModule) : BaseRemoteProvider() {

    private val METADATA_PROP = "io.titan-data"
    private val gson = ModelTypeAdapters.configure(GsonBuilder()).create()

    fun getClient(remote: Remote, params: RemoteParameters): AmazonS3 {
        remote as S3Remote
        params as S3Parameters

        val accessKey = params.accessKey ?: remote.accessKey
            ?: throw IllegalArgumentException("missing access key")
        val secretKey = params.secretKey ?: remote.secretKey
            ?: throw IllegalArgumentException("missing secret key")
        val region = params.region ?: remote.region
            ?: throw IllegalArgumentException("missing region")

        val creds = when (params.sessionToken) {
            null -> BasicAWSCredentials(accessKey, secretKey)
            else -> BasicSessionCredentials(accessKey, secretKey, params.sessionToken)
        }
        val provider = AWSStaticCredentialsProvider(creds)

        return AmazonS3ClientBuilder.standard().withCredentials(provider).withRegion(region).build()!!
    }

    fun getPath(remote: Remote, commitId: String? = null): Pair<String, String?> {
        remote as S3Remote
        val key = when (remote.path) {
            null -> commitId
            else -> when (commitId) {
                null -> remote.path
                else -> "${remote.path}/$commitId"
            }
        }

        return Pair(remote.bucket, key)
    }

    private fun objectToCommit(obj: ObjectMetadata): Commit? {
        val metadata = obj.userMetadata
        if (metadata == null || !metadata.containsKey(METADATA_PROP)) {
            return null
        }

        return gson.fromJson(metadata[METADATA_PROP], Commit::class.java)
    }

    private fun getMetadataKey(key: String?): String {
        return when (key) {
            null -> "titan"
            else -> "$key/titan"
        }
    }

    private fun getMetadataContent(remote: Remote, params: RemoteParameters): InputStream {
        val s3 = getClient(remote, params)
        val (bucket, key) = getPath(remote)

        try {
            return s3.getObject(bucket, getMetadataKey(key)).objectContent
        } catch (e: AmazonS3Exception) {
            if (e.statusCode == 404) {
                return ByteArrayInputStream("".toByteArray())
            } else {
                throw e
            }
        }
    }

    private fun appendMetadata(remote: Remote, params: RemoteParameters, json: String) {
        val s3 = getClient(remote, params)
        val (bucket, key) = getPath(remote)

        var length = 0L
        var currentMetadata: InputStream
        try {
            val obj = s3.getObject(bucket, getMetadataKey(key))
            currentMetadata = obj.objectContent
            length = obj.objectMetadata.contentLength
        } catch (e: AmazonS3Exception) {
            if (e.statusCode == 404) {
                currentMetadata = ByteArrayInputStream("".toByteArray())
            } else {
                throw e
            }
        }

        val appendStream = ByteArrayInputStream("$json\n".toByteArray())
        val stream = SequenceInputStream(currentMetadata, appendStream)
        var objectMetadata = ObjectMetadata()
        objectMetadata.contentLength = length + json.length + 1
        s3.putObject(PutObjectRequest(bucket, getMetadataKey(key), stream, objectMetadata))
    }

    override fun listCommits(remote: Remote, params: RemoteParameters, tags: List<String>?): List<Commit> {
        val metadata = getMetadataContent(remote, params)
        val ret = mutableListOf<Commit>()

        for (line in metadata.bufferedReader().lines()) {
            if (line != "") {
                ret.add(gson.fromJson(line, Commit::class.java))
            }
        }
        metadata.close()

        return TagFilter(tags).filter(ret)
    }

    override fun getCommit(remote: Remote, commitId: String, params: RemoteParameters): Commit {
        val s3 = getClient(remote, params)
        val (bucket, key) = getPath(remote, commitId)
        try {
            val obj = s3.getObjectMetadata(bucket, key)

            return objectToCommit(obj)
                    ?: throw NoSuchObjectException("no such commit $commitId in remote '${remote.name}'")
        } catch (e: AmazonS3Exception) {
            if (e.statusCode == 404) {
                throw NoSuchObjectException("no such commit $commitId in remote '${remote.name}'")
            }
            throw e
        }
    }

    // TODO refactor this into manageable chunks
    override fun runOperation(operation: OperationExecutor) {
        val s3 = getClient(operation.remote, operation.params)
        val (bucket, key) = getPath(operation.remote, operation.operation.commitId)
        val repo = operation.repo
        val operationId = operation.operation.id

        val scratch = providers.storage.createOperationScratch(repo, operationId)
        try {
            val base = providers.storage.mountOperationVolumes(repo, operationId)
            try {
                for (vol in providers.storage.listVolumes(repo)) {
                    val desc = vol.properties?.get("path")?.toString() ?: vol.name

                    if (operation.operation.type == Operation.Type.PUSH) {

                        operation.addProgress(ProgressEntry(type = ProgressEntry.Type.START,
                                message = "Creating archive for $desc"))

                        val archive = "$scratch/${vol.name}.tar.gz"
                        val args = arrayOf("tar", "czf", archive, ".")
                        val process = ProcessBuilder()
                                .directory(File("$base/${vol.name}"))
                                .command(*args)
                                .start()
                        providers.commandExecutor.exec(process, args.joinToString())
                        operation.addProgress(ProgressEntry(type = ProgressEntry.Type.END))

                        operation.addProgress(ProgressEntry(type = ProgressEntry.Type.START,
                                message = "Uploading archive for $desc"))
                        // TODO - progress monitoring
                        s3.putObject(bucket, "$key/${vol.name}.tar.gz", File(archive))
                        operation.addProgress(ProgressEntry(type = ProgressEntry.Type.END))
                    } else {
                        operation.addProgress(ProgressEntry(type = ProgressEntry.Type.START,
                                message = "Downloading archive for $desc"))

                        val archive = "$scratch/${vol.name}.tar.gz"
                        val obj = s3.getObject(bucket, "$key/${vol.name}.tar.gz")
                        // TODO - progress monitoring
                        obj.objectContent.use { input ->
                            File(archive).outputStream().use { output ->
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
                }

                if (operation.operation.type == Operation.Type.PUSH) {
                    val commit = providers.storage.getCommit(repo, operation.operation.commitId)
                    val metadata = ObjectMetadata()
                    val json = gson.toJson(commit)
                    metadata.userMetadata = mapOf(METADATA_PROP to json)
                    metadata.contentLength = 0
                    val input = ByteArrayInputStream("".toByteArray())
                    val request = PutObjectRequest(bucket, key, input, metadata)
                    s3.putObject(request)

                    appendMetadata(operation.remote, operation.params, json)
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
