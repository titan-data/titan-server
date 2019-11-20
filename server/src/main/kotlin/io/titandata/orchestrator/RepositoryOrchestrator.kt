package io.titandata.orchestrator

import io.titandata.ServiceLocator
import io.titandata.models.Repository
import io.titandata.models.RepositoryStatus
import org.jetbrains.exposed.sql.transactions.transaction

class RepositoryOrchestrator(val services: ServiceLocator) {

    fun createRepository(repo: Repository) {
        NameUtil.validateRepoName(repo.name)
        val volumeSet = transaction {
            services.metadata.createRepository(repo)
            val vs = services.metadata.createVolumeSet(repo.name, null, true)
            vs
        }
        services.context.createVolumeSet(volumeSet)
    }

    fun listRepositories(): List<Repository> {
        return transaction {
            services.metadata.listRepositories()
        }
    }

    fun getRepository(name: String): Repository {
        NameUtil.validateRepoName(name)
        return transaction {
            services.metadata.getRepository(name)
        }
    }

    fun getRepositoryStatus(name: String): RepositoryStatus {
        NameUtil.validateRepoName(name)
        val volumeSet = transaction {
            services.metadata.getActiveVolumeSet(name)
        }
        return transaction {
            RepositoryStatus(
                    lastCommit = services.metadata.getLastCommit(name),
                    sourceCommit = services.metadata.getCommitSource(volumeSet)
            )
        }
    }

    fun updateRepository(name: String, repo: Repository) {
        NameUtil.validateRepoName(name)
        NameUtil.validateRepoName(repo.name)
        transaction {
            services.metadata.updateRepository(name, repo)
        }
    }

    fun deleteRepository(name: String) {
        NameUtil.validateRepoName(name)
        transaction {
            services.metadata.markAllVolumeSetsDeleting(name)
            services.metadata.deleteRepository(name)
        }
        services.reaper.signal()
    }
}
