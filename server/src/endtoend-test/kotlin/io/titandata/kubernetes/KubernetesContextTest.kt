package io.titandata.kubernetes

import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import io.kubernetes.client.ApiException
import io.kubernetes.client.apis.CoreV1Api
import io.titandata.context.kubernetes.KubernetesCsiContext
import java.util.UUID

/**
 * This test suite is a little different from the other endtoend tests in that it's not connecting to a fully
 * instantiated titan server. Rather, it's testing the internals of the KubernetesContext driver with a real
 * k8s environment (hence the reason for it being an end2end test).
 */
class KubernetesContextTest : StringSpec() {

    private val context = KubernetesCsiContext()
    private val coreApi: CoreV1Api = CoreV1Api()
    private val vs = UUID.randomUUID().toString()
    private val namespace = "default"

    private lateinit var volumeConfig: Map<String, Any>

    // TODO - clean up any partially created resources "vs-"

    init {
        "create volume succeeds" {
            volumeConfig = context.createVolume(vs, "test")
            volumeConfig["pvc"] shouldBe "$vs-test"
            var phase = "Pending"
            while (phase == "Pending") {
                val result = coreApi.readNamespacedPersistentVolumeClaimStatus("$vs-test", namespace, null)
                phase = result.status.phase
                Thread.sleep(1000)
            }
            phase shouldBe "Bound"
        }

        "delete volume succeeds" {
            context.deleteVolume(vs, "test", volumeConfig)
            val exception = shouldThrow<ApiException> {
                coreApi.readNamespacedPersistentVolumeClaimStatus("$vs-test", namespace, null)
            }
            exception.code shouldBe 404
        }
    }
}
