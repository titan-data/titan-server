/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.storage.zfs

import com.google.gson.GsonBuilder
import com.google.gson.JsonSyntaxException
import com.google.gson.reflect.TypeToken
import io.titandata.exception.CommandException
import io.titandata.exception.InvalidStateException
import io.titandata.exception.NoSuchObjectException
import io.titandata.exception.ObjectExistsException
import io.titandata.models.Commit
import io.titandata.models.CommitStatus
import io.titandata.models.Operation
import io.titandata.models.Remote
import io.titandata.models.Repository
import io.titandata.models.Volume
import io.titandata.serialization.ModelTypeAdapters
import io.titandata.storage.OperationData
import io.titandata.storage.StorageProvider
import io.titandata.util.CommandExecutor
import io.titandata.util.GuidGenerator
import org.slf4j.LoggerFactory

/**
 * ZFS Storage provider. This implements all of the local repository operations on top of a ZFS
 * storage pool. By design, it stores everything it needs within the ZFS dataset structure and
 * properties, though it would be possible to add an in-memory cache (or even a full database)
 * should it be required. Another area of future investigation could be the use of channel
 * programs to collapse multiple ZFS commands into single atomic operations.
 *
 * To keep down code sprawl, each group of operations (repositories, commits, operations, volumes)
 * is separated out into a separate class, and this base provider simply keeps common helper
 * functions and redirects to those sub-classes.
 */
class ZfsStorageProvider(
    val poolName: String = "titan",
    val timestampProperty: String = "timestamp"
) : StorageProvider {

    companion object {
        val log = LoggerFactory.getLogger(ZfsStorageProvider::class.java)
    }

    enum class ObjectType(val displayName: String) {
        REPOSITORY("repository"),
        COMMIT("commit"),
        VOLUME("volume"),
        OPERATION("operation")
    }

    internal val METADATA_PROP = "io.titan-data:metadata"
    internal val ACTIVE_PROP = "io.titan-data:active"
    internal val REMOTES_PROP = "io.titan-data:remotes"
    internal val OPERATION_PROP = "io.titan-data:operation"
    internal val INITIAL_COMMIT = "initial"
    internal val executor = CommandExecutor()
    internal val generator = GuidGenerator()

    internal val volumeManager = ZfsVolumeManager(this)
    internal val operationManager = ZfsOperationManager(this)
    internal val repositoryManager = ZfsRepositoryManager(this)
    internal val commitManager = ZfsCommitManager(this)

    internal val gson = ModelTypeAdapters.configure(GsonBuilder()).create()

    // Utility methods that translate from generic CommandExceptions into a more specific
    // exception based on error output

    fun checkNoSuchObject(e: CommandException, name: String, type: ObjectType) {
        if ("does not exist" in e.output) {
            throw NoSuchObjectException("no such ${type.displayName} '$name'")
        }
    }

    fun checkNoSuchRepository(e: CommandException, name: String) {
        checkNoSuchObject(e, name, ObjectType.REPOSITORY)
    }

    fun checkNoSuchVolume(e: CommandException, name: String) {
        checkNoSuchObject(e, name, ObjectType.VOLUME)
    }

    fun checkObjectExists(e: CommandException, name: String, type: ObjectType) {
        if ("already exists" in e.output) {
            throw ObjectExistsException("${type.displayName} '$name' already exists")
        }
    }

    fun checkRepositoryExists(e: CommandException, name: String) {
        checkObjectExists(e, name, ObjectType.REPOSITORY)
    }

    fun checkCommitExists(e: CommandException, name: String) {
        checkObjectExists(e, name, ObjectType.COMMIT)
    }

    fun checkVolumeExists(e: CommandException, name: String) {
        checkObjectExists(e, name, ObjectType.VOLUME)
    }

    /**
     * Parse metadata for the given dataset. It should never be the case that someone can
     * configure invalid metadata, but just in case we convert any JSON parsing errors into a
     * more generic InvalidStateException that is used across the server.
     */
    fun parseMetadata(metadata: String): Map<String, Any> {
        try {
            return gson.fromJson(metadata, object : TypeToken<Map<String, Any>>() {}.type)
        } catch (e: JsonSyntaxException) {
            throw InvalidStateException("metadata must be valid JSON")
        }
    }

    /**
     * Validate that the given name (for repository or commit hash) is a valid ZFS dataset name.
     * The underlying commands would fail if we didn't do this, but this provides a much nicer
     * error message to the user.
     */
    fun validateName(name: String, type: ObjectType, allowInitialCommit: Boolean = false) {
        val regex = "^[a-zA-Z0-9_\\-:.]+$".toRegex()
        if (!regex.matches(name)) {
            throw IllegalArgumentException("invalid ${type.displayName} name, can only contain " +
                    "alphanumeric characters, '-', ':', '.', or '_'")
        }
        if (type == ObjectType.COMMIT && name == INITIAL_COMMIT && !allowInitialCommit) {
            throw IllegalArgumentException("commit id cannot be reserved id '$INITIAL_COMMIT'")
        }
        if (type == ObjectType.VOLUME && name.startsWith("_")) {
            throw IllegalArgumentException("volume names cannot start with '_'")
        }
    }

    /**
     * Helper method to wrap validateName() for repositories
     */
    fun validateRepositoryName(name: String) {
        validateName(name, ObjectType.REPOSITORY)
    }

    /**
     * Helper method to wrap validateName() for commits
     */
    fun validateCommitName(name: String, allowInitialCommit: Boolean = false) {
        validateName(name, ObjectType.COMMIT, allowInitialCommit)
    }

    /**
     * Helper method to wrap validateName() for volumes
     */
    fun validateVolumeName(name: String) {
        validateName(name, ObjectType.VOLUME)
    }

    /**
     * Helper method to get the current active dataset for a given repo. We accomplish this by
     * looking at the io.titan-data:active property.
     */
    fun getActive(name: String): String {
        try {
            return executor.exec("zfs", "list", "-Hpo", "io.titan-data:active",
                    "$poolName/repo/$name").trim()
        } catch (e: CommandException) {
            checkNoSuchRepository(e, name)
            throw e
        }
    }

    /**
     * Helper function that returns the GUID of the active dataset within the repo that contains
     * the commit. We invoke 'zfs list -t snapshot -d 2' to print out all datasets a depth of two
     * beneath the repository. We then parse each name, looking for the matching hash, and then
     * breaking up the dataset name into components to find the guid. This will return null if no
     * matching snapshot is found.
     */
    fun getCommitGuid(repo: String, id: String, allowInitialCommit: Boolean = false): String? {
        validateRepositoryName(repo)
        validateCommitName(id, allowInitialCommit)
        try {
            val output = executor.exec("zfs", "list", "-Ho", "name,defer_destroy",
                    "-t", "snapshot", "-d", "2", "$poolName/repo/$repo")
            val regex = "^$poolName/repo/$repo/([^@]+)@$id\toff$".toRegex(RegexOption.MULTILINE)
            val result = regex.find(output)
            return result?.groupValues?.get(1)
        } catch (e: CommandException) {
            checkNoSuchRepository(e, repo)
            throw e
        }
    }

    /**
     * Helper method to create a clone of a whole guid from the given hash. This will create a
     * empty GUID dataset, and then clone any datasets beneath it.
     */
    fun cloneCommit(repo: String, guid: String, hash: String, newGuid: String) {
        val output = executor.exec("zfs", "list", "-rHo", "name,$METADATA_PROP",
                "$poolName/repo/$repo/$guid")
        executor.exec("zfs", "create", "$poolName/repo/$repo/$newGuid")
        val regex = "^$poolName/repo/$repo/$guid/([^/]+)\t(.*)$".toRegex()
        for (line in output.lines()) {
            val result = regex.find(line.trim())
            if (result != null) {
                val volumeName = result.groupValues[1]
                val metadata = result.groupValues[2]
                executor.exec("zfs", "clone", "-o", "$METADATA_PROP=$metadata",
                        "$poolName/repo/$repo/$guid/$volumeName@$hash", "$poolName/repo/$repo/$newGuid/$volumeName")
            }
        }
    }

    /**
     * Get the base mountpoint for a volume or entire repository.
     */
    fun getMountpoint(repo: String, volume: String? = null): String {
        if (volume == null) {
            return "/var/lib/$poolName/mnt/$repo"
        } else {
            return "/var/lib/$poolName/mnt/$repo/$volume"
        }
    }

    /**
     * Helper function to help with debugging EBUSY mount failures. This will unmmount the directory, but if it
     * fails with EBUSY, then we'll automatically log the results of lsof to see if there is any process currenly
     * using it. If the path is not currently mounted, then we just ignore it and move on.
     */
    fun safeUnmount(path: String) {
        try {
            executor.exec("umount", path)
        } catch (e: CommandException) {
            if ("not mounted" in e.output) {
                return // Ignore
            } else if ("target is busy" in e.output) {
                try {
                    log.info(executor.exec("lsof"))
                } catch (ex: CommandException) {
                    // Ignore
                }
            }
            throw e
        }
    }

    /**
     * This is a helper function that is used for "zfs set" operations that can contain sensitive information
     * (such as credentials in remotes or remote parameters). We apply a large hammer here and just blank out
     * the whole value. If this proves problematic, then we can implement a more fine-grained scheme that pulls in
     * more of the remote-specific context to only blank out sensitive fields.
     */
    fun secureZfsSet(property: String, value: String, target: String): String {
        val process = executor.start("zfs", "set", "$property=$value", target)
        val argString = "zfs, set, $property=*****, $target"
        return executor.exec(process, argString)
    }

    /*
     * The remainder of this class simply redirects methods to the appropriate helper class. They
     * are all annotated as synchronized as a simple (if incomplete) guard to prevent concurrent
     * modifications. For the load we expect to place on the server, this should be sufficient. Of
     * course, it doesn't actually make the operations atomic, so it's very possible that should
     * the server fail in between a series of ZFS updates, it could leave the system in an
     * incomplete state. Fully solving this is beyond the current server scope, but could
     * theoretically be solved through ZFS channel programs or application-level recovery.
     */

    @Synchronized
    override fun createRepository(repo: Repository) {
        log.info("create repository ${repo.name}")
        repositoryManager.createRepository(repo)
    }

    @Synchronized
    override fun listRepositories(): List<Repository> {
        return repositoryManager.listRepositories()
    }

    @Synchronized
    override fun getRepository(name: String): Repository {
        return repositoryManager.getRepository(name)
    }

    @Synchronized
    override fun updateRepository(name: String, repo: Repository) {
        log.info("update repository $name")
        repositoryManager.updateRepository(name, repo)
    }

    @Synchronized
    override fun deleteRepository(name: String) {
        log.info("delete repository $name")
        repositoryManager.deleteRepository(name)
    }

    @Synchronized
    override fun getRemotes(repo: String): List<Remote> {
        return repositoryManager.getRemotes(repo)
    }

    @Synchronized
    override fun updateRemotes(repo: String, remotes: List<Remote>) {
        log.info("update remotes for repository $repo")
        repositoryManager.updateRemotes(repo, remotes)
    }

    @Synchronized
    override fun createCommit(repo: String, commit: Commit): Commit {
        log.info("create commit ${commit.id} in $repo")
        return commitManager.createCommit(repo, commit)
    }

    @Synchronized
    override fun getCommit(repo: String, id: String): Commit {
        return commitManager.getCommit(repo, id)
    }

    @Synchronized
    override fun getCommitStatus(repo: String, id: String): CommitStatus {
        return commitManager.getCommitStatus(repo, id)
    }

    @Synchronized
    override fun listCommits(repo: String): List<Commit> {
        return commitManager.listCommits(repo)
    }

    @Synchronized
    override fun deleteCommit(repo: String, commit: String) {
        log.info("delete commit $commit in $repo")
        commitManager.deleteCommit(repo, commit)
    }

    @Synchronized
    override fun checkoutCommit(repo: String, commit: String) {
        log.info("checkout commit $commit in $repo")
        commitManager.checkoutCommit(repo, commit)
    }

    @Synchronized
    override fun createOperation(repo: String, operation: OperationData, localCommit: String?) {
        log.info("create operation ${operation.operation.id} in $repo")
        operationManager.createOperation(repo, operation, localCommit)
    }

    @Synchronized
    override fun listOperations(repo: String): List<OperationData> {
        return operationManager.listOperations(repo)
    }

    @Synchronized
    override fun getOperation(repo: String, id: String): OperationData {
        return operationManager.getOperation(repo, id)
    }

    @Synchronized
    override fun commitOperation(repo: String, id: String, commit: Commit) {
        log.info("commit operation $id in $repo with commit ${commit.id}")
        operationManager.commitOperation(repo, id, commit)
    }

    @Synchronized
    override fun discardOperation(repo: String, id: String) {
        log.info("discard operation $id in $repo")
        operationManager.discardOperation(repo, id)
    }

    override fun updateOperationState(repo: String, id: String, state: Operation.State) {
        operationManager.updateOperationState(repo, id, state)
    }

    @Synchronized
    override fun mountOperationVolumes(repo: String, id: String): String {
        log.info("mount volumes for operation $id in $repo")
        return operationManager.mountOperationVolumes(repo, id)
    }

    @Synchronized
    override fun unmountOperationVolumes(repo: String, id: String) {
        log.info("unmount volumes for operation $id in $repo")
        operationManager.unmountOperationVolumes(repo, id)
    }

    override fun createOperationScratch(repo: String, id: String): String {
        log.info("create scratch space for operation $id in $repo")
        return operationManager.createOperationScratch(repo, id)
    }

    override fun destroyOperationScratch(repo: String, id: String) {
        log.info("destroy scratch space for operation $id in $repo")
        operationManager.destroyOperationScratch(repo, id)
    }

    @Synchronized
    override fun createVolume(repo: String, name: String, properties: Map<String, Any>): Volume {
        log.info("create volume $name in $repo")
        return volumeManager.createVolume(repo, name, properties)
    }

    @Synchronized
    override fun deleteVolume(repo: String, name: String) {
        log.info("delete volume $name in $repo")
        return volumeManager.deleteVolume(repo, name)
    }

    @Synchronized
    override fun getVolume(repo: String, name: String): Volume {
        return volumeManager.getVolume(repo, name)
    }

    @Synchronized
    override fun mountVolume(repo: String, name: String): Volume {
        log.info("mount volume $name in $repo")
        return volumeManager.mountVolume(repo, name)
    }

    @Synchronized
    override fun unmountVolume(repo: String, name: String) {
        log.info("unmount volume $name in $repo")
        volumeManager.unmountVolume(repo, name)
    }

    @Synchronized
    override fun listVolumes(repo: String): List<Volume> {
        return volumeManager.listVolumes(repo)
    }
}
