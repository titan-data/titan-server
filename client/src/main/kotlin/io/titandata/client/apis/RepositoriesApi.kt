/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.client.apis

import io.titandata.client.infrastructure.ApiClient
import io.titandata.client.infrastructure.ClientException
import io.titandata.client.infrastructure.RequestConfig
import io.titandata.client.infrastructure.RequestMethod
import io.titandata.client.infrastructure.ResponseType
import io.titandata.client.infrastructure.ServerException
import io.titandata.client.infrastructure.Success
import io.titandata.models.Repository

class RepositoriesApi(basePath: String = "http://localhost:5001") : ApiClient(basePath) {

    @Suppress("UNCHECKED_CAST")
    fun createRepository(repository: Repository) : Repository {
        val localVariableBody: Any? = repository
        val localVariableQuery: Map<String,List<String>> = mapOf()
        val localVariableHeaders: MutableMap<String, String> = mutableMapOf()
        val localVariableConfig = RequestConfig(
                RequestMethod.POST,
                "/v1/repositories",
                query = localVariableQuery,
                headers = localVariableHeaders
        )
        val response = request<Repository>(
            localVariableConfig,
            localVariableBody
        )

        return when (response.responseType) {
            ResponseType.Success -> (response as Success<*>).data as Repository
            ResponseType.ClientError -> throw ClientException.fromResponse(gson, response)
            ResponseType.ServerError -> throw ServerException.fromResponse(gson, response)
            else -> throw NotImplementedError(response.responseType.toString())
        }
    }

    fun deleteRepository(repositoryName: String) {
        val localVariableBody: Any? = null
        val localVariableQuery: Map<String,List<String>> = mapOf()
        val localVariableHeaders: MutableMap<String, String> = mutableMapOf()
        val localVariableConfig = RequestConfig(
                RequestMethod.DELETE,
                "/v1/repositories/{repositoryName}".replace("{" + "repositoryName" + "}", "$repositoryName"),
                query = localVariableQuery,
                headers = localVariableHeaders
        )
        val response = request<Any?>(
            localVariableConfig,
            localVariableBody
        )

        return when (response.responseType) {
            ResponseType.Success -> Unit
            ResponseType.ClientError -> throw ClientException.fromResponse(gson, response)
            ResponseType.ServerError -> throw ServerException.fromResponse(gson, response)
            else -> throw NotImplementedError(response.responseType.toString())
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun getRepository(repositoryName: String) : Repository {
        val localVariableBody: Any? = null
        val localVariableQuery: Map<String,List<String>> = mapOf()
        val localVariableHeaders: MutableMap<String, String> = mutableMapOf()
        val localVariableConfig = RequestConfig(
                RequestMethod.GET,
                "/v1/repositories/{repositoryName}".replace("{" + "repositoryName" + "}", "$repositoryName"),
                query = localVariableQuery,
                headers = localVariableHeaders
        )
        val response = request<Repository>(
            localVariableConfig,
            localVariableBody
        )

        return when (response.responseType) {
            ResponseType.Success -> (response as Success<*>).data as Repository
            ResponseType.ClientError -> throw ClientException.fromResponse(gson, response)
            ResponseType.ServerError -> throw ServerException.fromResponse(gson, response)
            else -> throw NotImplementedError(response.responseType.toString())
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun listRepositories() : Array<Repository> {
        val localVariableBody: Any? = null
        val localVariableQuery: Map<String,List<String>> = mapOf()
        val localVariableHeaders: MutableMap<String, String> = mutableMapOf()
        val localVariableConfig = RequestConfig(
                RequestMethod.GET,
                "/v1/repositories",
                query = localVariableQuery,
                headers = localVariableHeaders
        )
        val response = request<Array<Repository>>(
            localVariableConfig,
            localVariableBody
        )

        return when (response.responseType) {
            ResponseType.Success -> (response as Success<*>).data as Array<Repository>
            ResponseType.ClientError -> throw ClientException.fromResponse(gson, response)
            ResponseType.ServerError -> throw ServerException.fromResponse(gson, response)
            else -> throw NotImplementedError(response.responseType.toString())
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun updateRepository(repositoryName: String, repository: Repository) : Repository {
        val localVariableBody: Any? = repository
        val localVariableQuery: Map<String,List<String>> = mapOf()
        val localVariableHeaders: MutableMap<String, String> = mutableMapOf()
        val localVariableConfig = RequestConfig(
                RequestMethod.POST,
                "/v1/repositories/{repositoryName}".replace("{" + "repositoryName" + "}", "$repositoryName"),
                query = localVariableQuery,
                headers = localVariableHeaders
        )
        val response = request<Repository>(
            localVariableConfig,
            localVariableBody
        )

        return when (response.responseType) {
            ResponseType.Success -> (response as Success<*>).data as Repository
            ResponseType.ClientError -> throw ClientException.fromResponse(gson, response)
            ResponseType.ServerError -> throw ServerException.fromResponse(gson, response)
            else -> throw NotImplementedError(response.responseType.toString())
        }
    }
}
