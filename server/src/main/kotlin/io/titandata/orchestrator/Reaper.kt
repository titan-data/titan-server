package io.titandata.orchestrator

import io.titandata.ProviderModule
import io.titandata.metadata.MetadataProvider
import org.jetbrains.exposed.sql.transactions.transaction
import org.slf4j.LoggerFactory
import java.util.concurrent.locks.Condition
import java.util.concurrent.locks.ReentrantLock

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
            var deleted = 1
            while (true) {
                if (deleted == 0) {
                    cv.await()
                }
                try {
                    deleted = reapCommits()
                } catch (e: Throwable) {
                    log.error("error during reaping", e)
                }
            }
        } finally {
            lock.unlock()
        }
    }

    fun reapCommits() : Int {
        val commits = transaction {
            providers.metadata.listDeletingCommits().filter {
                !providers.metadata.hasClones(it)
            }
        }

        var deleted = 0
        for (c in commits) {
            val volumes = transaction {
                providers.metadata.listVolumes(c.volumeSet).map { it.name }
            }
            try {
                providers.storage.deleteCommit(c.volumeSet, c.guid, volumes)
                deleted++
            } catch (e: Throwable) {
                log.error("error deleting commit ${c.guid}", e)
            }
        }

        return deleted
    }

    // TODO - reap volumesets
}