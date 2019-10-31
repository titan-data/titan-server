package io.titandata.orchestrator

import io.titandata.ProviderModule
import io.titandata.models.Commit
import io.titandata.models.CommitStatus

class CommitOrchestrator(val providers: ProviderModule) {

    fun createCommit(repo: String, commit: Commit): Commit {
        return providers.storage.createCommit(repo, commit)
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
        return providers.storage.deleteCommit(repo, commit)
    }

    fun checkoutCommit(repo: String, commit: String) {
        providers.storage.checkoutCommit(repo, commit)
    }

    fun updateCommit(repo: String, commit: Commit) {
        providers.storage.updateCommit(repo, commit)
    }
}
