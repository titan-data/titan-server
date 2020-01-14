package io.titandata.kubernetes

import io.kubernetes.client.openapi.apis.CoreV1Api
import io.kubernetes.client.openapi.models.V1ContainerBuilder
import io.kubernetes.client.openapi.models.V1ObjectMetaBuilder
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaimVolumeSourceBuilder
import io.kubernetes.client.openapi.models.V1PodBuilder
import io.kubernetes.client.openapi.models.V1PodSpecBuilder
import io.kubernetes.client.openapi.models.V1VolumeBuilder
import io.kubernetes.client.openapi.models.V1VolumeMountBuilder
import io.titandata.EndToEndTest
import io.titandata.context.kubernetes.KubernetesCsiContext
import io.titandata.shell.CommandExecutor

/**
 * This test suite is a little different from the other endtoend tests in that it's not connecting to a fully
 * instantiated titan server. Rather, it's testing the internals of the KubernetesContext driver with a real
 * k8s environment (hence the reason for it being an end2end test).
 */
abstract class KubernetesTest : EndToEndTest("kubernetes-csi") {

    internal val coreApi: CoreV1Api
    internal val executor = CommandExecutor()
    private val  namespace = "default"

    internal fun getPodStatus(name: String): Boolean {
        val pod = coreApi.readNamespacedPod(name, namespace, null, null, null)
        val statuses = pod?.status?.containerStatuses ?: return false
        for (container in statuses) {
            if (container.restartCount != 0) {
                throw Exception("container ${container.name} restarted ${container.restartCount} times")
            }
            if (!container.ready) {
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
        coreApi.createNamespacedPod(namespace, request, null, null, null)
    }
}
