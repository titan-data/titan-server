/*
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */

package com.delphix.titan

import com.delphix.titan.client.apis.CommitsApi
import com.delphix.titan.client.apis.OperationsApi
import com.delphix.titan.client.apis.RemotesApi
import com.delphix.titan.client.apis.RepositoriesApi
import com.delphix.titan.client.apis.VolumeApi
import com.delphix.titan.models.ProgressEntry
import io.kotlintest.TestCaseOrder
import io.kotlintest.specs.StringSpec
import org.slf4j.LoggerFactory

abstract class EndToEndTest : StringSpec() {

    val dockerUtil = DockerUtil()
    val url = "http://localhost:${dockerUtil.port}"
    val repoApi = RepositoriesApi(url)
    val volumeApi = VolumeApi(url)
    val commitApi = CommitsApi(url)
    val remoteApi = RemotesApi(url)
    val operationApi = OperationsApi(url)

    override fun testCaseOrder() = TestCaseOrder.Sequential

    companion object {
        val log = LoggerFactory.getLogger(EndToEndTest::class.java)
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
