package io.titandata.kubernetes

import io.kotlintest.matchers.string.shouldContain
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kubernetes.client.ApiException
import io.titandata.models.Volume
import io.titandata.remote.RemoteOperation
import io.titandata.remote.RemoteOperationType
import io.titandata.remote.RemoteProgress
import io.titandata.remote.nop.server.NopRemoteServer
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

        /*
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

         */

        "syncVolumes succeeds" {
            val provider = NopRemoteServer()
            val operation = RemoteOperation(
                    updateProgress = { _: RemoteProgress, _: String?, _: Int? -> Unit },
                    operationId = vs,
                    commitId = "commit",
                    commit = emptyMap(),
                    remote = emptyMap(),
                    parameters = emptyMap(),
                    type = RemoteOperationType.PUSH
            )
            context.syncVolumes(provider, operation, emptyList(), Volume("test", emptyMap(), volumeConfig))
        }

        /*
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
                coreApi.readNamespacedPersistentVolumeClaimStatus("$vs2-test", context.namespace, null)
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

         */

        "delete volume succeeds" {
            context.deleteVolume(vs, "test", volumeConfig)
            val exception = shouldThrow<ApiException> {
                coreApi.readNamespacedPersistentVolumeClaimStatus("$vs-test", context.namespace, null)
            }
            exception.code shouldBe 404
        }

        "delete of non-existent volume succeeds" {
            context.deleteVolume(vs, "test", volumeConfig)
        }
    }
}
