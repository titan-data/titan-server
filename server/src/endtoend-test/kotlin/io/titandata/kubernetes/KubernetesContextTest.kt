package io.titandata.kubernetes

import io.kotlintest.matchers.string.shouldContain
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kubernetes.client.ApiException
import io.titandata.shell.CommandException
import java.util.UUID

/**
 * This test suite is a little different from the other endtoend tests in that it's not connecting to a fully
 * instantiated titan server. Rather, it's testing the internals of the KubernetesContext driver with a real
 * k8s environment (hence the reason for it being an end2end test).
 */
class KubernetesContextTest : KubernetesTest() {

    private val vs = UUID.randomUUID().toString()
    private val vs2 = UUID.randomUUID().toString()

    private lateinit var volumeConfig: Map<String, Any>
    private lateinit var cloneConfig: Map<String, Any>

    init {
        "create volume succeeds" {
            volumeConfig = context.createVolume(vs, "test")
            volumeConfig["pvc"] shouldBe "$vs-test"
            waitForVolume(vs, "test", volumeConfig)
        }

        "create pod succeeds" {
            launchPod("$vs-test", "$vs-test")
            waitForPod("$vs-test")
        }

        "write file contents succeeds" {
            executor.exec("kubectl", "exec", "$vs-test", "--", "sh", "-c", "echo test > /data/out; sync; sleep 1;")
        }

        "create commit succeeds" {
            context.commitVolume(vs, "id", "test", volumeConfig)
            waitForCommit(vs, "id", listOf("test"))
        }

        "delete pod succeeds" {
            executor.exec("kubectl", "delete", "pod", "--grace-period=0", "--force", "$vs-test")
        }

        "clone volume succeeds" {
            cloneConfig = context.cloneVolume(vs, "id", vs2, "test", volumeConfig)
            waitForVolume(vs2, "test", cloneConfig)
        }

        "create cloned pod succeeds" {
            launchPod("$vs2-test", "$vs2-test")
            waitForPod("$vs2-test")
        }

        "file contents are correct" {
            val output = executor.exec("kubectl", "exec", "$vs2-test", "cat", "/data/out").trim()
            output shouldBe "test"
        }

        "delete cloned pod succeeds" {
            executor.exec("kubectl", "delete", "pod", "--grace-period=0", "--force", "$vs2-test")
        }

        "delete cloned volumed succeeds" {
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
