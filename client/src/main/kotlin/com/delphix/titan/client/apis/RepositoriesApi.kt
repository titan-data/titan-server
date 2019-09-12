package com.delphix.titan.client.apis

import com.delphix.titan.client.infrastructure.ApiClient
import com.delphix.titan.client.infrastructure.ClientException
import com.delphix.titan.client.infrastructure.MultiValueMap
import com.delphix.titan.client.infrastructure.RequestConfig
import com.delphix.titan.client.infrastructure.RequestMethod
import com.delphix.titan.client.infrastructure.ResponseType
import com.delphix.titan.client.infrastructure.ServerException
import com.delphix.titan.client.infrastructure.Success
import com.delphix.titan.models.Repository

class RepositoriesApi(basePath: String = "http://localhost:5001") : ApiClient(basePath) {

    @Suppress("UNCHECKED_CAST")
    fun createRepository(repository: Repository) : Repository {
        val localVariableBody: Any? = repository
        val localVariableQuery: MultiValueMap = mapOf()
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
        val localVariableQuery: MultiValueMap = mapOf()
        val localVariableHeaders: MutableMap<String, String> = mutableMapOf()
        val localVariableConfig = RequestConfig(
            RequestMethod.DELETE,
            "/v1/repositories/{repositoryName}".replace("{"+"repositoryName"+"}", "$repositoryName"),
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
        val localVariableQuery: MultiValueMap = mapOf()
        val localVariableHeaders: MutableMap<String, String> = mutableMapOf()
        val localVariableConfig = RequestConfig(
            RequestMethod.GET,
            "/v1/repositories/{repositoryName}".replace("{"+"repositoryName"+"}", "$repositoryName"),
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
        val localVariableQuery: MultiValueMap = mapOf()
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
        val localVariableQuery: MultiValueMap = mapOf()
        val localVariableHeaders: MutableMap<String, String> = mutableMapOf()
        val localVariableConfig = RequestConfig(
            RequestMethod.POST,
            "/v1/repositories/{repositoryName}".replace("{"+"repositoryName"+"}", "$repositoryName"),
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
