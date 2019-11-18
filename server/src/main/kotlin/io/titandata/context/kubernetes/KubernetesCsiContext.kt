package io.titandata.context.kubernetes

import com.google.gson.JsonSyntaxException
import io.kubernetes.client.ApiException
import io.kubernetes.client.Configuration.setDefaultApiClient
import io.kubernetes.client.apis.CoreV1Api
import io.kubernetes.client.custom.Quantity
import io.kubernetes.client.models.V1ObjectMetaBuilder
import io.kubernetes.client.models.V1PersistentVolumeClaimBuilder
import io.kubernetes.client.models.V1PersistentVolumeClaimSpecBuilder
import io.kubernetes.client.models.V1ResourceRequirementsBuilder
import io.kubernetes.client.util.Config
import io.titandata.context.RuntimeContext
import io.titandata.models.CommitStatus
import io.titandata.models.RepositoryVolumeStatus
import org.slf4j.LoggerFactory

/**
 * Kubernetes runtime context. In a k8s environment, we leverage CSI (Container Storage Interface) drivers to do
 * snapshot and cloning of data for us. Each volume is a PersistentVolumeClaim, while every commit is a
 * VolumeSnapshot.
 */
class KubernetesCsiContext : RuntimeContext {
    private var coreApi: CoreV1Api
    private val namespace = "default"

    companion object {
        val log = LoggerFactory.getLogger(KubernetesCsiContext::class.java)
    }

    init {
        val client = Config.defaultClient()
        setDefaultApiClient(client)
        coreApi = CoreV1Api()
    }

    override fun createVolumeSet(volumeSet: String) {
        // Nothing to do
    }

    override fun cloneVolumeSet(sourceVolumeSet: String, sourceCommit: String, newVolumeSet: String) {
        // Nothing to do
    }

    override fun deleteVolumeSet(volumeSet: String) {
        // Nothing to do
    }

    override fun createVolume(volumeSet: String, volumeName: String): Map<String, Any> {
        val name = "$volumeSet-$volumeName"
        val request = V1PersistentVolumeClaimBuilder()
                .withMetadata(
                        V1ObjectMetaBuilder()
                                .withName(name)
                                .withLabels(mapOf("titanVolume" to volumeName))
                                .build()
                )
                .withSpec(
                        V1PersistentVolumeClaimSpecBuilder()
                                .withAccessModes("ReadWriteOnce")
                                .withResources(
                                        V1ResourceRequirementsBuilder()
                                                .withRequests(mapOf("storage" to Quantity.fromString("1Gi")))
                                                .build()
                                )
                                .build())
                .build()
        val claim = coreApi.createNamespacedPersistentVolumeClaim(namespace, request, null, null, null)
        log.info("Created PersistentVolumeClaim '$name', status = ${claim.status.phase}")
        return mapOf("pvc" to name)
    }

    override fun deleteVolume(volumeSet: String, volumeName: String, config: Map<String, Any>) {
        val pvc = config["pvc"] as? String ?: throw IllegalStateException("missing or invalid pvc name in volume config")
        /*
         * Delete will always fail parsing the response, due to a limitation of  the swagger-generated client:
         *
         *      https://github.com/kubernetes-client/java/issues/86
         *
         * There's not much we can do other than to catch the error and then query the PVC to see if it still remains.
         */
        try {
            coreApi.deleteNamespacedPersistentVolumeClaim(pvc, namespace, null, null, null, null, null, null)
        } catch (e: JsonSyntaxException) {
            // Ignore
        }
        try {
            val result = coreApi.readNamespacedPersistentVolumeClaimStatus(pvc, namespace, null)
            throw IllegalStateException("Unable to delete PersistentVolumeClaim '$pvc', status = ${result.status.phase}")
        } catch (e: ApiException) {
            if (e.code != 404) {
                throw e
            }
        }

        log.info("Deleted PersistentVolumeClaim '$pvc'")
    }

    override fun cloneVolume(sourceVolumeSet: String, sourceCommit: String, newVolumeSet: String, volumeName: String): Map<String, Any> {
        TODO("not implemented")
    }

    override fun getVolumeStatus(volumeSet: String, volume: String): RepositoryVolumeStatus {
        TODO("not implemented")
    }

    override fun createCommit(volumeSet: String, commitId: String, volumeNames: List<String>) {
        TODO("not implemented")
    }

    override fun getCommitStatus(volumeSet: String, commitId: String, volumeNames: List<String>): CommitStatus {
        TODO("not implemented")
    }

    override fun deleteCommit(volumeSet: String, commitId: String, volumeNames: List<String>) {
        TODO("not implemented")
    }

    override fun activateVolume(volumeSet: String, volumeName: String, config: Map<String, Any>) {
        TODO("not implemented")
    }

    override fun deactivateVolume(volumeSet: String, volumeName: String, config: Map<String, Any>) {
        TODO("not implemented")
    }
}
