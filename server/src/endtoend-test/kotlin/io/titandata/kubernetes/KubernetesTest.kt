package io.titandata.kubernetes

import io.kubernetes.client.apis.CoreV1Api
import io.kubernetes.client.models.V1ContainerBuilder
import io.kubernetes.client.models.V1ObjectMetaBuilder
import io.kubernetes.client.models.V1PersistentVolumeClaimVolumeSourceBuilder
import io.kubernetes.client.models.V1PodBuilder
import io.kubernetes.client.models.V1PodSpecBuilder
import io.kubernetes.client.models.V1VolumeBuilder
import io.kubernetes.client.models.V1VolumeMountBuilder
import io.titandata.EndToEndTest
import io.titandata.context.kubernetes.KubernetesCsiContext
import io.titandata.shell.CommandExecutor

/**
 * This test suite is a little different from the other endtoend tests in that it's not connecting to a fully
 * instantiated titan server. Rather, it's testing the internals of the KubernetesContext driver with a real
 * k8s environment (hence the reason for it being an end2end test).
 */
abstract class KubernetesTest : EndToEndTest("kubernetes-csi") {

    internal val context: KubernetesCsiContext
    internal val coreApi: CoreV1Api
    internal val executor = CommandExecutor()

    init {
        context = KubernetesCsiContext()
        coreApi = CoreV1Api()
    }

    internal fun waitForVolume(volumeSet: String, volumeName: String, config: Map<String, Any>) {
        var status = context.getVolumeStatus(volumeSet, volumeName, config)
        while (!status.ready) {
            if (status.error != null) {
                throw Exception(status.error)
            }
            status = context.getVolumeStatus(volumeSet, volumeName, config)
            if (!status.ready) {
                Thread.sleep(1000)
            }
        }
    }

    internal fun waitForVolumeApi(repository: String, volumeName: String) {
        var status = volumeApi.getVolumeStatus(repository, volumeName)
        while (!status.ready) {
            if (status.error != null) {
                throw Exception(status.error)
            }
            status = volumeApi.getVolumeStatus(repository, volumeName)
            if (!status.ready) {
                Thread.sleep(1000)
            }
        }
    }

    internal fun waitForCommit(volumeSet: String, commitId: String, volumeNames: List<String>) {
        var status = context.getCommitStatus(volumeSet, commitId, volumeNames)
        while (!status.ready) {
            if (status.error != null) {
                throw Exception(status.error)
            }
            status = context.getCommitStatus(volumeSet, commitId, volumeNames)
            if (!status.ready) {
                Thread.sleep(1000)
            }
        }
    }

    internal fun waitForCommitApi(repo: String, commitId: String) {
        var status = commitApi.getCommitStatus(repo, commitId)
        while (!status.ready) {
            if (status.error != null) {
                throw Exception(status.error)
            }
            status = commitApi.getCommitStatus(repo, commitId)
            if (!status.ready) {
                Thread.sleep(1000)
            }
        }
    }

    internal fun getPodStatus(name: String): Boolean {
        val pod = coreApi.readNamespacedPod(name, context.namespace, null, null, null)
        val statuses = pod?.status?.containerStatuses ?: return false
        for (container in statuses) {
            if (container.restartCount != 0) {
                throw Exception("container ${container.name} restarted ${container.restartCount} times")
            }
            if (!container.isReady) {
                return false
            }
        }
        return true
    }

    internal fun waitForPod(name: String) {
        var ready = getPodStatus(name)
        while (!ready) {
            Thread.sleep(1000)
            ready = getPodStatus(name)
        }
    }

    internal fun launchPod(name: String, claim: String) {
        val request = V1PodBuilder()
                .withMetadata(V1ObjectMetaBuilder().withName(name).build())
                .withSpec(V1PodSpecBuilder()
                        .withContainers(V1ContainerBuilder()
                                .withName("test")
                                .withImage("ubuntu:bionic")
                                .withCommand("/bin/sh")
                                .withArgs("-c", "while true; do sleep 5; done")
                                .withVolumeMounts(V1VolumeMountBuilder()
                                        .withName("data")
                                        .withMountPath("/data")
                                        .build())
                                .build())
                        .withVolumes(V1VolumeBuilder()
                                .withName("data")
                                .withPersistentVolumeClaim(V1PersistentVolumeClaimVolumeSourceBuilder().withClaimName(claim).build())
                                .build())
                        .build())
                .build()
        coreApi.createNamespacedPod(context.namespace, request, null, null, null)
    }
}
