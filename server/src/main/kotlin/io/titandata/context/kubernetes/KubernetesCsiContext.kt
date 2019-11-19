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
import io.titandata.shell.CommandException
import io.titandata.shell.CommandExecutor
import org.slf4j.LoggerFactory

/**
 * Kubernetes runtime context. In a k8s environment, we leverage CSI (Container Storage Interface) drivers to do
 * snapshot and cloning of data for us. Each volume is a PersistentVolumeClaim, while every commit is a
 * VolumeSnapshot.
 */
class KubernetesCsiContext : RuntimeContext {
    private var coreApi: CoreV1Api
    private val defaultNamespace = "default"
    private val executor = CommandExecutor()

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

    override fun commitVolumeSet(volumeSet: String, commitId: String) {
        // Nothing to do
    }

    override fun deleteVolumeSetCommit(volumeSet: String, commitId: String) {
        // Nothing to do
    }

    override fun createVolume(volumeSet: String, volumeName: String): Map<String, Any> {
        val name = "$volumeSet-$volumeName"
        val size = "1Gi"
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
                                                .withRequests(mapOf("storage" to Quantity.fromString(size)))
                                                .build()
                                )
                                .build())
                .build()
        val claim = coreApi.createNamespacedPersistentVolumeClaim(defaultNamespace, request, null, null, null)
        log.info("Created PersistentVolumeClaim '$name', status = ${claim.status.phase}")
        return mapOf(
                "pvc" to name,
                "namespace" to defaultNamespace,
                "size" to size)
    }

    override fun deleteVolume(volumeSet: String, volumeName: String, config: Map<String, Any>) {
        val pvc = config["pvc"] as? String ?: throw IllegalStateException("missing or invalid pvc name in volume config")
        val namespace = config["namespace"] as? String ?: throw IllegalStateException("missing or invalid namespace in volume config")

        /*
         * Delete will always fail parsing the response, due to a limitation of  the swagger-generated client:
         *
         *      https://github.com/kubernetes-client/java/issues/86
         *
         * There's not much we can do other than to catch the error and then query the PVC to see if it still remains.
         */
        try {
            try {
                coreApi.deleteNamespacedPersistentVolumeClaim(pvc, namespace, null, null, null, null, null, null)
            } catch (e: JsonSyntaxException) {
                // Ignore
            }

            val result = coreApi.readNamespacedPersistentVolumeClaimStatus(pvc, namespace, null)
            throw IllegalStateException("Unable to delete PersistentVolumeClaim '$pvc', status = ${result.status.phase}")
        } catch (e: ApiException) {
            // Deletion is idempotent, so this is designed to ignore not found errors from the original pvc deletion
            if (e.code != 404) {
                throw e
            }
        }

        log.info("Deleted PersistentVolumeClaim '$pvc'")
    }

    /**
     * The VolumeSnapshot APIs are currently in alpha and not part of the published REST API (even using
     * StorageV1alpha1), so we have to invoke kubectl in order to manage them. This should be updated to use the native
     * API when it is available.
     */
    override fun commitVolume(volumeSet: String, commitId: String, volumeName: String, config: Map<String, Any>) {
        val pvc = config["pvc"] as? String ?: throw IllegalStateException("missing or invalid pvc name in volume config")
        val size = config["size"] as? String ?: throw IllegalStateException("missing or invalid size in volume config")
        val name = "$pvc-$commitId"

        val file = createTempFile()
        try {
            file.writeText("apiVersion: snapshot.storage.k8s.io/v1alpha1\n" +
                    "kind: VolumeSnapshot\n" +
                    "metadata:\n" +
                    "  name: $name\n" +
                    "  labels:\n" +
                    "    titanVolume: $volumeName\n" +
                    "    titanCommit: $commitId\n" +
                    "    titanSize: $size\n" +
                    "spec:\n" +
                    "  source:\n" +
                    "    kind: PersistentVolumeClaim\n" +
                    "    name: $pvc\n"
            )

            executor.exec("kubectl", "apply", "-f", file.path)
        } finally {
            file.delete()
        }
    }

    override fun deleteVolumeCommit(volumeSet: String, commitId: String, volumeName: String) {
        try {
            executor.exec("kubectl", "delete", "volumesnapshot", "$volumeSet-$volumeName-$commitId")
        } catch (e: CommandException) {
            // Deletion is idempotent, so ignore errors if it doesn't exist
            if (!e.output.contains("NotFound")) {
                throw e
            }
        }
    }

    override fun cloneVolume(
        sourceVolumeSet: String,
        sourceCommit: String,
        newVolumeSet: String,
        volumeName: String,
        sourceConfig: Map<String, Any>
    ): Map<String, Any> {
        TODO("not implemented")
    }

    override fun getVolumeStatus(volumeSet: String, volume: String): RepositoryVolumeStatus {
        TODO("not implemented")
    }

    override fun getCommitStatus(volumeSet: String, commitId: String, volumeNames: List<String>): CommitStatus {
        TODO("not implemented")
    }

    override fun activateVolume(volumeSet: String, volumeName: String, config: Map<String, Any>) {
        TODO("not implemented")
    }

    override fun deactivateVolume(volumeSet: String, volumeName: String, config: Map<String, Any>) {
        TODO("not implemented")
    }
}
