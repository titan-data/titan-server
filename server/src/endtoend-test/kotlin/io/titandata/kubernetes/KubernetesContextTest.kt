package io.titandata.kubernetes

import com.google.gson.JsonParser
import io.kotlintest.matchers.string.shouldContain
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import io.kubernetes.client.ApiException
import io.kubernetes.client.apis.CoreV1Api
import io.titandata.context.kubernetes.KubernetesCsiContext
import io.titandata.shell.CommandException
import io.titandata.shell.CommandExecutor
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
    private val vs2 = UUID.randomUUID().toString()
    private val namespace = "default"
    private val executor = CommandExecutor()

    private lateinit var volumeConfig: Map<String, Any>
    private lateinit var cloneConfig: Map<String, Any>

    // TODO - clean up any partially created resources "vs-"

    private fun waitForVolume(name: String) {
        var phase = "Pending"
        while (phase == "Pending") {
            val result = coreApi.readNamespacedPersistentVolumeClaimStatus(name, namespace, null)
            phase = result.status.phase
            if (phase == "Pending") {
                Thread.sleep(1000)
            }
        }
        phase shouldBe "Bound"
    }

    private fun waitForSnapshot(name: String) {
        var ready = "false"
        while (ready == "false") {
            val output = executor.exec("kubectl", "get", "volumesnapshot", name, "-o", "json")
            val json = JsonParser.parseString(output)
            val status = json.asJsonObject.getAsJsonObject("status")
            if (status != null) {
                ready = status.get("readyToUse").asString
                val error = status.getAsJsonObject("error")?.get("message")?.asString
                error shouldBe null
            }
            if (ready == "false") {
                Thread.sleep(1000)
            }
        }
    }

    init {
        "create volume succeeds" {
            volumeConfig = context.createVolume(vs, "test")
            volumeConfig["pvc"] shouldBe "$vs-test"
            waitForVolume("$vs-test")
        }

        "create commit succeeds" {
            context.commitVolume(vs, "id", "test", volumeConfig)
            waitForSnapshot("$vs-test-id")
        }

        "clone volume succeeds" {
            cloneConfig = context.cloneVolume(vs, "id", vs2, "test", volumeConfig)
            waitForVolume("$vs2-test")
        }

        "delete clone succeeds" {
            context.deleteVolume(vs2, "test", cloneConfig)
            val exception = shouldThrow<ApiException> {
                coreApi.readNamespacedPersistentVolumeClaimStatus("$vs2-test", namespace, null)
            }
            exception.code shouldBe 404
        }

        "delete commit succeeds" {
            context.deleteVolumeCommit(vs, "id", "test")
            val exception = shouldThrow<CommandException> {
                executor.exec("kubectl", "get", "volumesnapshot", "$vs-test-id")
            }
            exception.output shouldContain "NotFound"
        }

        "delete of non-existent commit succeeds" {
            context.deleteVolumeCommit(vs, "id", "test")
        }

        "delete volume succeeds" {
            context.deleteVolume(vs, "test", volumeConfig)
            val exception = shouldThrow<ApiException> {
                coreApi.readNamespacedPersistentVolumeClaimStatus("$vs-test", namespace, null)
            }
            exception.code shouldBe 404
        }

        "delete of non-existent volume succeeds" {
            context.deleteVolume(vs, "test", volumeConfig)
        }
    }
}
