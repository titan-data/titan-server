package io.titandata.orchestrator

import io.titandata.ProviderModule
import io.titandata.models.Repository
import io.titandata.models.RepositoryStatus

class RepositoryOrchestrator(val providers: ProviderModule) {

    fun createRepository(repo: Repository) {
        providers.storage.createRepository(repo)
    }

    fun listRepositories(): List<Repository> {
        return providers.storage.listRepositories()
    }

    fun getRepository(name: String): Repository {
        return providers.storage.getRepository(name)
    }

    fun getRepositoryStatus(name: String): RepositoryStatus {
        return providers.storage.getRepositoryStatus(name)
    }

    fun updateRepository(name: String, repo: Repository) {
        providers.storage.updateRepository(name, repo)
    }

    fun deleteRepository(name: String) {
        providers.storage.deleteRepository(name)
    }
}
