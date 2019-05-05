package com.delphix.titan.client.apis

import com.delphix.titan.client.infrastructure.ApiClient
import com.delphix.titan.client.infrastructure.ClientException
import com.delphix.titan.client.infrastructure.MultiValueMap
import com.delphix.titan.client.infrastructure.RequestConfig
import com.delphix.titan.client.infrastructure.RequestMethod
import com.delphix.titan.client.infrastructure.ResponseType
import com.delphix.titan.client.infrastructure.ServerException
import com.delphix.titan.client.infrastructure.Success
import com.delphix.titan.models.Commit

class CommitsApi(basePath: String = "http://localhost:5001") : ApiClient(basePath) {

    fun checkoutCommit(repositoryName: String, commitId: String) {
        val localVariableBody: Any? = null
        val localVariableQuery: MultiValueMap = mapOf()
        val localVariableHeaders: MutableMap<String, String> = mutableMapOf()
        val localVariableConfig = RequestConfig(
            RequestMethod.POST,
            "/v1/repositories/{repositoryName}/commits/{commitId}/checkout".replace("{"+"repositoryName"+"}", "$repositoryName").replace("{"+"commitId"+"}", "$commitId"),
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
    fun createCommit(repositoryName: String, commit: Commit) : Commit {
        val localVariableBody: Any? = commit
        val localVariableQuery: MultiValueMap = mapOf()
        val localVariableHeaders: MutableMap<String, String> = mutableMapOf()
        val localVariableConfig = RequestConfig(
            RequestMethod.POST,
            "/v1/repositories/{repositoryName}/commits".replace("{"+"repositoryName"+"}", "$repositoryName"),
            query = localVariableQuery,
            headers = localVariableHeaders
        )
        val response = request<Commit>(
            localVariableConfig,
            localVariableBody
        )

        return when (response.responseType) {
            ResponseType.Success -> (response as Success<*>).data as Commit
            ResponseType.ClientError -> throw ClientException.fromResponse(gson, response)
            ResponseType.ServerError -> throw ServerException.fromResponse(gson, response)
            else -> throw NotImplementedError(response.responseType.toString())
        }
    }

    fun deleteCommit(repositoryName: String, commitId: String) {
        val localVariableBody: Any? = null
        val localVariableQuery: MultiValueMap = mapOf()
        val localVariableHeaders: MutableMap<String, String> = mutableMapOf()
        val localVariableConfig = RequestConfig(
            RequestMethod.DELETE,
            "/v1/repositories/{repositoryName}/commits/{commitId}".replace("{"+"repositoryName"+"}", "$repositoryName").replace("{"+"commitId"+"}", "$commitId"),
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
    fun getCommit(repositoryName: String, commitId: String) : Commit {
        val localVariableBody: Any? = null
        val localVariableQuery: MultiValueMap = mapOf()
        val localVariableHeaders: MutableMap<String, String> = mutableMapOf()
        val localVariableConfig = RequestConfig(
            RequestMethod.GET,
            "/v1/repositories/{repositoryName}/commits/{commitId}".replace("{"+"repositoryName"+"}", "$repositoryName").replace("{"+"commitId"+"}", "$commitId"),
            query = localVariableQuery,
            headers = localVariableHeaders
        )
        val response = request<Commit>(
            localVariableConfig,
            localVariableBody
        )

        return when (response.responseType) {
            ResponseType.Success -> (response as Success<*>).data as Commit
            ResponseType.ClientError -> throw ClientException.fromResponse(gson, response)
            ResponseType.ServerError -> throw ServerException.fromResponse(gson, response)
            else -> throw NotImplementedError(response.responseType.toString())
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun listCommits(repositoryName: String) : Array<Commit> {
        val localVariableBody: Any? = null
        val localVariableQuery: MultiValueMap = mapOf()
        val localVariableHeaders: MutableMap<String, String> = mutableMapOf()
        val localVariableConfig = RequestConfig(
            RequestMethod.GET,
            "/v1/repositories/{repositoryName}/commits".replace("{"+"repositoryName"+"}", "$repositoryName"),
            query = localVariableQuery,
            headers = localVariableHeaders
        )
        val response = request<Array<Commit>>(
            localVariableConfig,
            localVariableBody
        )

        return when (response.responseType) {
            ResponseType.Success -> (response as Success<*>).data as Array<Commit>
            ResponseType.ClientError -> throw ClientException.fromResponse(gson, response)
            ResponseType.ServerError -> throw ServerException.fromResponse(gson, response)
            else -> throw NotImplementedError(response.responseType.toString())
        }
    }

}
