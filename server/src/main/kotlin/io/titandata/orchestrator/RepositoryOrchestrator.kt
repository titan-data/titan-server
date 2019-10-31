package io.titandata.orchestrator

import io.titandata.ProviderModule
import io.titandata.models.Repository
import io.titandata.models.RepositoryStatus
import org.jetbrains.exposed.sql.transactions.transaction

class RepositoryOrchestrator(val providers: ProviderModule) {

    internal val nameRegex = "^[a-zA-Z0-9_\\-:.]+$".toRegex()

    private fun validateRepoName(repoName: String) {
        if (!nameRegex.matches(repoName)) {
            throw IllegalArgumentException("invalid repository name, can only contain " +
                    "alphanumeric characters, '-', ':', '.', or '_'")
        }
        if (repoName.length > 64) {
            throw IllegalArgumentException("invalid repository name, must be 64 characters or less")
        }
    }

    fun createRepository(repo: Repository) {
        validateRepoName(repo.name)
        transaction {
            providers.metadata.createRepository(repo)
        }
        providers.storage.createRepository(repo)
    }

    fun listRepositories(): List<Repository> {
        return transaction {
            providers.metadata.listRepositories()
        }
    }

    fun getRepository(name: String): Repository {
        validateRepoName(name)
        return transaction {
            providers.metadata.getRepository(name)
        }
    }

    fun getRepositoryStatus(name: String): RepositoryStatus {
        validateRepoName(name)
        return providers.storage.getRepositoryStatus(name)
    }

    fun updateRepository(name: String, repo: Repository) {
        validateRepoName(name)
        validateRepoName(repo.name)
        providers.metadata.updateRepository(name, repo)
    }

    fun deleteRepository(name: String) {
        validateRepoName(name)
        transaction {
            providers.metadata.deleteRepository(name)
        }
        providers.storage.deleteRepository(name)
    }
}
