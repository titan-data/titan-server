package io.titandata.orchestrator

import io.titandata.ProviderModule
import java.util.concurrent.locks.ReentrantLock
import org.jetbrains.exposed.sql.transactions.transaction
import org.slf4j.LoggerFactory

/*
 * The reaper orchestrator is the class that asynchronously looks for objects in the DELETING state that can be
 * safely deleted, invoking the appropriate storage provider method to do so. The reaper runs asynchronously in
 * a separate thread, and is poked whwnever an object is marked as deleting, and then continues to run as long
 * as there are objects to delete.
 *
 * It respects the ZFS dependency chain between commits and volumesets, ensuring that they're deleted
 * in that order. It will ensure that any clones of a commit are deleted before the commit itself is deleted.
 */
class Reaper(val providers: ProviderModule) : Runnable {
    private val lock = ReentrantLock()
    private val cv = lock.newCondition()

    companion object {
        val log = LoggerFactory.getLogger(Reaper::class.java)
    }

    fun signal() {
        lock.lock()
        cv.signal()
        lock.unlock()
    }

    override fun run() {
        lock.lock()
        try {
            var changed = true
            while (true) {
                if (!changed) {
                    cv.await()
                }

                log.debug("reaping storage objects")

                changed = false
                // First pass - delete commits without clones
                try {
                    changed = reapCommits() || changed
                } catch (e: Throwable) {
                    log.error("error during reaping", e)
                }

                // Next pass, mark any inactive volume sets that no longer have commits as deleting
                markEmptyVolumeSets()

                // Delete any volumes explicitly marked for deletion
                changed = reapVolumes() || changed

                // Now go and delete any volume sets that don't have commits
                changed = reapVolumeSets() || changed
            }
        } finally {
            lock.unlock()
        }
    }

    /*
     * When reaping commits, we look for any that have been marked deleting and do not have any clones.
     */
    fun reapCommits(): Boolean {
        val commits = transaction {
            providers.metadata.listDeletingCommits().filter {
                !providers.metadata.hasClones(it)
            }
        }

        var ret = false
        for (c in commits) {
            val volumes = transaction {
                providers.metadata.listVolumes(c.volumeSet).map { it.name }
            }
            try {
                providers.storage.deleteCommit(c.volumeSet, c.guid, volumes)
                transaction {
                    providers.metadata.deleteCommit(c)
                }
                ret = true
            } catch (e: Throwable) {
                log.error("error deleting commit ${c.guid}", e)
            }
        }

        return ret
    }

    fun markEmptyVolumeSets() {
        val volumeSets = transaction {
            providers.metadata.listInactiveVolumeSets().filter {
                providers.metadata.isVolumeSetEmpty(it) &&
                        !providers.metadata.operationExists(it)
            }
        }

        transaction {
            for (vs in volumeSets) {
                providers.metadata.markVolumeSetDeleting(vs)
            }
        }
    }

    fun reapVolumeSets(): Boolean {
        val volumeSets = transaction {
            providers.metadata.listDeletingVolumeSets().filter {
                providers.metadata.isVolumeSetEmpty(it)
            }
        }

        var ret = false
        for (vs in volumeSets) {
            try {
                val volumes = transaction {
                    providers.metadata.listVolumes(vs)
                }
                for (vol in volumes) {
                    providers.storage.deleteVolume(vs, vol.name, vol.config)
                    transaction {
                        providers.metadata.deleteVolume(vs, vol.name)
                    }
                }
                providers.storage.deleteVolumeSet(vs)
                transaction {
                    providers.metadata.deleteVolumeSet(vs)
                }
                ret = true
            } catch (t: Throwable) {
                log.error("error reaping volume set $vs", t)
            }
        }

        return ret
    }

    // We don't recursively mark volumes deleted, this is only true when volumes are explicitly deleted
    fun reapVolumes(): Boolean {
        val volumes = transaction {
            providers.metadata.listDeletingVolumes()
        }
        var ret = false
        for ((vs, vol) in volumes) {
            try {
                providers.storage.deleteVolume(vs, vol.name, vol.config)
                transaction {
                    providers.metadata.deleteVolume(vs, vol.name)
                }
                ret = true
            } catch (t: Throwable) {
                log.error("error reaping volume ${vol.name} in volume set $vs")
            }
        }
        return ret
    }
}
