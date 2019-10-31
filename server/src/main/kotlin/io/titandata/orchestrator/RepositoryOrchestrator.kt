package io.titandata.orchestrator

import io.titandata.ProviderModule
import io.titandata.models.Repository
import io.titandata.models.RepositoryStatus
import org.jetbrains.exposed.sql.transactions.transaction

class RepositoryOrchestrator(val providers: ProviderModule) {

    fun createRepository(repo: Repository) {
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
        return transaction {
            providers.metadata.getRepository(name)
        }
    }

    fun getRepositoryStatus(name: String): RepositoryStatus {
        return providers.storage.getRepositoryStatus(name)
    }

    fun updateRepository(name: String, repo: Repository) {
        providers.storage.updateRepository(name, repo)
    }

    fun deleteRepository(name: String) {
        transaction {
            providers.metadata.deleteRepository(name)
        }
        providers.storage.deleteRepository(name)
    }
}
