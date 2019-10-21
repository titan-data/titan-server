/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.storage.zfs

import io.titandata.exception.CommandException
import java.time.Duration
import org.slf4j.LoggerFactory

/*
 * The reaper class is responsible for cleaning up repositories commits that can't be deleted because there are
 * dependent clones. Most of the time, we are just deleting snapshots - which can be handled by by using the
 * deferred destroy capability of ZFS. But when we encounter a whole dataset that needs to be destroyed (such as
 * when we remove the last commit for a given checkout), that can then fail because there may be clones of those
 * deferred snapshots, and there is no mechanism to defer destroy a whole datset.
 *
 * Instead, we mark these datasets with the "io.titan-data:reap=on" property. The reaper then just periodically
 * scans all datasets to see if this property is set, and attempts to destroy them.
 */
class ZfsReaper(val provider: ZfsStorageProvider) : Runnable {

    private val DELAY = 300L
    private val poolName = provider.poolName
    private val REAPER_PROP = provider.REAPER_PROP

    companion object {
        val log = LoggerFactory.getLogger(ZfsCommitManager::class.java)
    }

    override fun run() {
        while (true) {
            reap()
            Thread.sleep(Duration.ofSeconds(DELAY).toMillis())
        }
    }

    fun reap() {
        val regex = "^(.*)\ton$".toRegex()
        val output = provider.executor.exec("zfs", "list", "-Hpo", "name,$REAPER_PROP", "-r", "-d", "2", "$poolName/repo")
        for (line in output.lines()) {
            val result = regex.find(line) ?: continue
            val dataset = result.groupValues.get(1)

            try {
                provider.executor.exec("zfs", "destroy", "-r", dataset)
                log.info("reaped dataset $dataset")
            } catch (e: CommandException) {
                // Ignore errors
            }
        }
    }
}
