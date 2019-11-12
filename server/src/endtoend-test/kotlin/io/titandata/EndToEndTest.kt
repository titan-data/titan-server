/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata

import io.kotlintest.TestCaseOrder
import io.kotlintest.specs.StringSpec
import io.titandata.client.apis.CommitsApi
import io.titandata.client.apis.OperationsApi
import io.titandata.client.apis.RemotesApi
import io.titandata.client.apis.RepositoriesApi
import io.titandata.client.apis.VolumesApi
import io.titandata.models.Commit
import io.titandata.models.ProgressEntry
import io.titandata.serialization.RemoteUtil
import org.slf4j.LoggerFactory

abstract class EndToEndTest : StringSpec() {

    val dockerUtil = DockerUtil()
    val remoteUtil = RemoteUtil()
    val url = "http://localhost:${dockerUtil.port}"
    val repoApi = RepositoriesApi(url)
    val volumeApi = VolumesApi(url)
    val commitApi = CommitsApi(url)
    val remoteApi = RemotesApi(url)
    val operationApi = OperationsApi(url)

    override fun testCaseOrder() = TestCaseOrder.Sequential

    companion object {
        val log = LoggerFactory.getLogger(EndToEndTest::class.java)
    }

    fun getTag(commit: Commit, key: String): String? {
        @Suppress("UNCHECKED_CAST")
        val tags = commit.properties["tags"] as Map<String, String>
        return tags.get(key)
    }

    fun waitForOperation(id: String): List<ProgressEntry> {
        var completed = false
        val result = mutableListOf<ProgressEntry>()
        while (!completed) {
            val progress = operationApi.getProgress("foo", id)
            result.addAll(progress)
            for (p in progress) {
                when (p.type) {
                    ProgressEntry.Type.COMPLETE -> completed = true
                    ProgressEntry.Type.ABORT -> throw Exception("operation aborted: ${p.message}")
                    ProgressEntry.Type.FAILED -> throw Exception("operation failed: ${p.message}")
                    else -> log.info(p.message)
                }
            }
            if (!completed) {
                Thread.sleep(500)
            }
        }
        return result
    }
}
