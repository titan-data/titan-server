/*
* Copyright The Titan Project Contributors.
 */

package io.titandata.kubernetes

import io.kotlintest.Spec
import io.kotlintest.shouldBe

class KubernetesConfigTest : KubernetesTest() {

    override fun beforeSpec(spec: Spec) {
        dockerUtil.stopServer()
    }

    override fun afterSpec(spec: Spec) {
        dockerUtil.stopServer(ignoreExceptions = false)
    }

    val configFile = "config"
    val kubeContext = executor.exec("kubectl", "config", "current-context").trim()
    val storageClass = "noSuchClass"
    val snapshotClass = "noSuchClass"

    init {
        "start server with configuration succeeds" {
            dockerUtil.startServer("configFile=$configFile", "context=$kubeContext",
                    "storageClass=$storageClass", "snapshotClass=$snapshotClass")
            dockerUtil.waitForServer()
        }

        "get context returns custom configuration" {
            val context = contextApi.getContext()
            context.properties.size shouldBe 4
            context.properties["configFile"] shouldBe configFile
            context.properties["context"] shouldBe kubeContext
            context.properties["storageClass"] shouldBe storageClass
            context.properties["snapshotClass"] shouldBe snapshotClass
        }
    }
}
