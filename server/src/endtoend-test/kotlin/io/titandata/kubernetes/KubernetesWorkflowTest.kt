/*
* Copyright The Titan Project Contributors.
 */

package io.titandata.kubernetes

import io.kotlintest.Spec
import io.kotlintest.matchers.string.shouldStartWith
import io.titandata.EndToEndTest

class KubernetesWorkflowTest : EndToEndTest() {

    override fun beforeSpec(spec: Spec) {
        kubernetesUtil.stopServer()
        kubernetesUtil.startServer()
        kubernetesUtil.waitForServer()
    }

    override fun afterSpec(spec: Spec) {
        kubernetesUtil.stopServer(ignoreExceptions = false)
    }

    init {
        "kubectl works" {
            val output = kubernetesUtil.execServer("kubectl", "get", "pods")
            output.shouldStartWith("NAME")
        }
    }
}