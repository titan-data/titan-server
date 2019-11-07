package io.titandata.orchestrator

import io.titandata.ProviderModule
import io.titandata.models.Commit
import io.titandata.models.Repository
import io.titandata.models.RepositoryStatus
import org.jetbrains.exposed.sql.transactions.transaction

class RepositoryOrchestrator(val providers: ProviderModule) {

    fun createRepository(repo: Repository) {
        NameUtil.validateRepoName(repo.name)
        val initialCommit = Commit(id="initial", properties=emptyMap())
        val volumeSet = transaction {
            providers.metadata.createRepository(repo)
            val vs = providers.metadata.createVolumeSet(repo.name, true)
            providers.metadata.createCommit(repo.name, vs, initialCommit)
            vs
        }
        providers.storage.createRepository(repo, volumeSet)
        providers.storage.createCommit(repo.name, volumeSet, initialCommit)
    }

    fun listRepositories(): List<Repository> {
        return transaction {
            providers.metadata.listRepositories()
        }
    }

    fun getRepository(name: String): Repository {
        NameUtil.validateRepoName(name)
        return transaction {
            providers.metadata.getRepository(name)
        }
    }

    fun getRepositoryStatus(name: String): RepositoryStatus {
        NameUtil.validateRepoName(name)
        val volumeSet = transaction {
            providers.metadata.getActiveVolumeSet(name)
        }
        val status = providers.storage.getRepositoryStatus(name, volumeSet)
        return transaction {
            RepositoryStatus(
                    logicalSize = status.logicalSize,
                    actualSize = status.actualSize,
                    volumeStatus = status.volumeStatus,
                    lastCommit = providers.metadata.getLastCommit(name),
                    sourceCommit = providers.metadata.getCommitSource(volumeSet)
            )
        }
    }

    fun updateRepository(name: String, repo: Repository) {
        NameUtil.validateRepoName(name)
        NameUtil.validateRepoName(repo.name)
        transaction {
            providers.metadata.updateRepository(name, repo)
        }
    }

    fun deleteRepository(name: String) {
        NameUtil.validateRepoName(name)
        transaction {
            providers.metadata.deleteRepository(name)
        }
        providers.storage.deleteRepository(name)
    }
}
