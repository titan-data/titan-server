package io.titandata.context.kubernetes

import com.google.gson.JsonObject
import com.google.gson.JsonParser
import io.kubernetes.client.Configuration.setDefaultApiClient
import io.kubernetes.client.apis.CoreV1Api
import io.kubernetes.client.apis.StorageV1Api
import io.kubernetes.client.custom.Quantity
import io.kubernetes.client.models.V1ObjectMetaBuilder
import io.kubernetes.client.models.V1PersistentVolumeClaimBuilder
import io.kubernetes.client.models.V1PersistentVolumeClaimSpecBuilder
import io.kubernetes.client.models.V1ResourceRequirementsBuilder
import io.kubernetes.client.util.Config
import io.titandata.context.RuntimeContext
import io.titandata.models.CommitStatus
import io.titandata.models.VolumeStatus
import io.titandata.shell.CommandException
import io.titandata.shell.CommandExecutor
import org.slf4j.LoggerFactory

/**
 * Kubernetes runtime context. In a k8s environment, we leverage CSI (Container Storage Interface) drivers to do
 * snapshot and cloning of data for us. Each volume is a PersistentVolumeClaim, while every commit is a
 * VolumeSnapshot.
 */
class KubernetesCsiContext(private val storageClass: String? = null, private val snapshotClass: String? = null) : RuntimeContext {
    private var coreApi: CoreV1Api
    private var storageApi: StorageV1Api
    val defaultNamespace = "default"
    private val executor = CommandExecutor()

    companion object {
        val log = LoggerFactory.getLogger(KubernetesCsiContext::class.java)
    }

    init {
        val client = Config.defaultClient()
        setDefaultApiClient(client)
        coreApi = CoreV1Api()
        storageApi = StorageV1Api()
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
        if (storageClass != null) {
            request.spec.storageClassName = storageClass
        }
        val claim = coreApi.createNamespacedPersistentVolumeClaim(defaultNamespace, request, null, null, null)
        log.info("Created PersistentVolumeClaim '$name', status = ${claim.status.phase}")
        return mapOf(
                "pvc" to name,
                "namespace" to defaultNamespace,
                "size" to size)
    }

    override fun deleteVolume(volumeSet: String, volumeName: String, config: Map<String, Any>) {
        val pvc = config["pvc"] as? String ?: throw IllegalStateException("missing or invalid pvc name in volume config")

        // Java client has issues with deletion (https://github.com/kubernetes-client/java/issues/86) so use kubectl
        try {
            executor.exec("kubectl", "delete", "--wait=false", "pvc", pvc)
        } catch (e: CommandException) {
            if (!e.output.contains("NotFound")) {
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
                    "    name: $pvc\n" +
                    if (snapshotClass != null) { "  snapshotClassName: $snapshotClass\n" } else { "" }
            )

            executor.exec("kubectl", "apply", "-f", file.path)
        } finally {
            file.delete()
        }
    }

    override fun deleteVolumeCommit(volumeSet: String, commitId: String, volumeName: String) {
        try {
            executor.exec("kubectl", "delete", "--wait=false", "volumesnapshot", "$volumeSet-$volumeName-$commitId")
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
        val size = sourceConfig["size"] as? String ?: throw IllegalStateException("missing or invalid size in volume config")
        val name = "$newVolumeSet-$volumeName"
        val snapshotName = "$sourceVolumeSet-$volumeName-$sourceCommit"

        val file = createTempFile()
        try {
            file.writeText("apiVersion: v1\n" +
                    "kind: PersistentVolumeClaim\n" +
                    "metadata:\n" +
                    "  name: $name\n" +
                    "  labels:\n" +
                    "    titanVolume: $volumeName\n" +
                    "spec:\n" +
                    "  dataSource:\n" +
                    "    kind: VolumeSnapshot\n" +
                    "    apiGroup: snapshot.storage.k8s.io\n" +
                    "    name: $snapshotName\n" +
                    "  accessModes:\n" +
                    "    - ReadWriteOnce\n" +
                    "  resources:\n" +
                    "    requests:\n" +
                    "      storage: $size\n"
            )

            executor.exec("kubectl", "apply", "-f", file.path)
        } finally {
            file.delete()
        }

        return mapOf(
                "pvc" to name,
                "namespace" to defaultNamespace,
                "size" to size)
    }

    override fun getVolumeStatus(volumeSet: String, volume: String, config: Map<String, Any>): VolumeStatus {
        val pvc = config["pvc"] as? String ?: throw IllegalStateException("missing or invalid pvc name in volume config")

        val claim = coreApi.readNamespacedPersistentVolumeClaimStatus(pvc, defaultNamespace, null)

        var okPhases = listOf("Pending", "Bound")
        var readyPhases = listOf("Pending", "Bound")
        if (claim.spec.storageClassName != null) {
            val storageClass = storageApi.readStorageClass(claim.spec.storageClassName, null, null, null)
            if (storageClass.volumeBindingMode == "Immediate") {
                readyPhases = listOf("Bound")
            }
        }

        val ready = claim.status.phase in readyPhases
        val error = if (claim.status.phase in okPhases) {
            null
        } else {
            "volume '$pvc' is in unknown state ${claim.status.phase}"
        }

        // Volumes can change size, this is just the original size
        val size = Quantity(config["size"] as String).number.toLong()
        return VolumeStatus(
                name = volume,
                logicalSize = size,
                actualSize = size,
                ready = ready,
                error = error
        )
    }

    private fun getSnapshot(name: String): JsonObject? {
        val output = executor.exec("kubectl", "get", "volumesnapshot", name, "-o", "json")
        val json = JsonParser.parseString(output)
        return json.asJsonObject
    }

    override fun getCommitStatus(volumeSet: String, commitId: String, volumeNames: List<String>): CommitStatus {
        var size = 0L
        var ready = true
        var error: String? = null
        for (v in volumeNames) {
            val snapshotName = "$volumeSet-$v-$commitId"
            val snapshot = getSnapshot(snapshotName)
            val status = snapshot?.getAsJsonObject("status")

            if (status == null) {
                ready = false
            } else if (status.get("readyToUse") != null) {
                if (status.get("readyToUse").asString != "true") {
                    ready = false
                }
                if (error == null) {
                    error = status.getAsJsonObject("error")?.get("message")?.asString
                }
            }

            val sizeLabel = snapshot?.getAsJsonObject("metadata")?.getAsJsonObject("labels")?.getAsJsonPrimitive("size")?.asString
            if (sizeLabel != null) {
                size += Quantity.fromString(sizeLabel).number.toLong()
            }
        }

        return CommitStatus(
                logicalSize = size,
                actualSize = size,
                uniqueSize = size,
                ready = ready,
                error = error
        )
    }

    override fun activateVolume(volumeSet: String, volumeName: String, config: Map<String, Any>) {
        // Nothing to do
    }

    override fun deactivateVolume(volumeSet: String, volumeName: String, config: Map<String, Any>) {
        // Nothing to do
    }
}
