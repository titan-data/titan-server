package io.titandata.orchestrator

import io.titandata.ProviderModule
import io.titandata.models.Commit
import io.titandata.models.CommitStatus
import org.jetbrains.exposed.sql.transactions.transaction

class CommitOrchestrator(val providers: ProviderModule) {

    fun createCommit(repo: String, commit: Commit): Commit {
        val activeVolumeSet = transaction {
            providers.metadata.getActiveVolumeSet(repo)
        }
        return providers.storage.createCommit(repo, activeVolumeSet, commit)
    }

    fun getCommit(repo: String, id: String): Commit {
        return providers.storage.getCommit(repo, id)
    }

    fun getCommitStatus(repo: String, id: String): CommitStatus {
        return providers.storage.getCommitStatus(repo, id)
    }

    fun listCommits(repo: String, tags: List<String>?): List<Commit> {
        return providers.storage.listCommits(repo, tags)
    }

    fun deleteCommit(repo: String, commit: String) {
        val activeVolumeSet = transaction {
            providers.metadata.getActiveVolumeSet(repo)
        }
        return providers.storage.deleteCommit(repo, activeVolumeSet, commit)
    }

    fun checkoutCommit(repo: String, commit: String) {
        val activeVolumeSet = transaction {
            providers.metadata.getActiveVolumeSet(repo)
        }
        val newVolumeSet = transaction {
            providers.metadata.createVolumeSet(repo)
        }
        providers.storage.checkoutCommit(repo, activeVolumeSet, newVolumeSet, commit)
        transaction {
            providers.metadata.activateVolumeSet(repo, newVolumeSet)
            providers.metadata.markVolumeSetDeleting(activeVolumeSet)
        }
    }

    fun updateCommit(repo: String, commit: Commit) {
        providers.storage.updateCommit(repo, commit)
    }
}
