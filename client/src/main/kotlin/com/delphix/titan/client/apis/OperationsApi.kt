package com.delphix.titan.client.apis

import com.delphix.titan.client.infrastructure.ApiClient
import com.delphix.titan.client.infrastructure.ClientException
import com.delphix.titan.client.infrastructure.MultiValueMap
import com.delphix.titan.client.infrastructure.RequestConfig
import com.delphix.titan.client.infrastructure.RequestMethod
import com.delphix.titan.client.infrastructure.ResponseType
import com.delphix.titan.client.infrastructure.ServerException
import com.delphix.titan.client.infrastructure.Success
import com.delphix.titan.models.Operation
import com.delphix.titan.models.RemoteParameters
import com.delphix.titan.models.ProgressEntry

class OperationsApi(basePath: String = "http://localhost:5001") : ApiClient(basePath) {

    fun deleteOperation(repositoryName: String, operationId: String) {
        val localVariableBody: Any? = null
        val localVariableQuery: MultiValueMap = mapOf()
        val localVariableHeaders: MutableMap<String, String> = mutableMapOf()
        val localVariableConfig = RequestConfig(
            RequestMethod.DELETE,
            "/v1/repositories/{repositoryName}/operations/{operationId}".replace("{"+"repositoryName"+"}", "$repositoryName").replace("{"+"operationId"+"}", "$operationId"),
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
    fun getOperation(repositoryName: String, operationId: String) : Operation {
        val localVariableBody: Any? = null
        val localVariableQuery: MultiValueMap = mapOf()
        val localVariableHeaders: MutableMap<String, String> = mutableMapOf()
        val localVariableConfig = RequestConfig(
            RequestMethod.GET,
            "/v1/repositories/{repositoryName}/operations/{operationId}".replace("{"+"repositoryName"+"}", "$repositoryName").replace("{"+"operationId"+"}", "$operationId"),
            query = localVariableQuery,
            headers = localVariableHeaders
        )
        val response = request<Operation>(
            localVariableConfig,
            localVariableBody
        )

        return when (response.responseType) {
            ResponseType.Success -> (response as Success<*>).data as Operation
            ResponseType.ClientError -> throw ClientException.fromResponse(gson, response)
            ResponseType.ServerError -> throw ServerException.fromResponse(gson, response)
            else -> throw NotImplementedError(response.responseType.toString())
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun getProgress(repositoryName: String, operationId: String) : Array<ProgressEntry> {
        val localVariableBody: Any? = null
        val localVariableQuery: MultiValueMap = mapOf()
        val localVariableHeaders: MutableMap<String, String> = mutableMapOf()
        val localVariableConfig = RequestConfig(
            RequestMethod.GET,
            "/v1/repositories/{repositoryName}/operations/{operationId}/progress".replace("{"+"repositoryName"+"}", "$repositoryName").replace("{"+"operationId"+"}", "$operationId"),
            query = localVariableQuery,
            headers = localVariableHeaders
        )
        val response = request<Array<ProgressEntry>>(
            localVariableConfig,
            localVariableBody
        )

        return when (response.responseType) {
            ResponseType.Success -> (response as Success<*>).data as Array<ProgressEntry>
            ResponseType.ClientError -> throw ClientException.fromResponse(gson, response)
            ResponseType.ServerError -> throw ServerException.fromResponse(gson, response)
            else -> throw NotImplementedError(response.responseType.toString())
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun listOperations(repositoryName: String) : Array<Operation> {
        val localVariableBody: Any? = null
        val localVariableQuery: MultiValueMap = mapOf()
        val localVariableHeaders: MutableMap<String, String> = mutableMapOf()
        val localVariableConfig = RequestConfig(
            RequestMethod.GET,
            "/v1/repositories/{repositoryName}/operations".replace("{"+"repositoryName"+"}", "$repositoryName"),
            query = localVariableQuery,
            headers = localVariableHeaders
        )
        val response = request<Array<Operation>>(
            localVariableConfig,
            localVariableBody
        )

        return when (response.responseType) {
            ResponseType.Success -> (response as Success<*>).data as kotlin.Array<Operation>
            ResponseType.ClientError -> throw ClientException.fromResponse(gson, response)
            ResponseType.ServerError -> throw ServerException.fromResponse(gson, response)
            else -> throw NotImplementedError(response.responseType.toString())
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun pull(repositoryName: String, remoteName: String, commitId: String, request: RemoteParameters) : Operation {
        val localVariableBody: Any? = request
        val localVariableQuery: MultiValueMap = mapOf()
        val localVariableHeaders: MutableMap<String, String> = mutableMapOf()
        val localVariableConfig = RequestConfig(
            RequestMethod.POST,
            "/v1/repositories/{repositoryName}/remotes/{remoteName}/commits/{commitId}/pull".replace("{"+"repositoryName"+"}", "$repositoryName").replace("{"+"remoteName"+"}", "$remoteName").replace("{"+"commitId"+"}", "$commitId"),
            query = localVariableQuery,
            headers = localVariableHeaders
        )
        val response = request<Operation>(
            localVariableConfig,
            localVariableBody
        )

        return when (response.responseType) {
            ResponseType.Success -> (response as Success<*>).data as Operation
            ResponseType.ClientError -> throw ClientException.fromResponse(gson, response)
            ResponseType.ServerError -> throw ServerException.fromResponse(gson, response)
            else -> throw NotImplementedError(response.responseType.toString())
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun push(repositoryName: String, remoteName: String, commitId: String, anyRequest: RemoteParameters?) : Operation {
        val localVariableBody: Any? = anyRequest
        val localVariableQuery: MultiValueMap = mapOf()
        val localVariableHeaders: MutableMap<String, String> = mutableMapOf()
        val localVariableConfig = RequestConfig(
            RequestMethod.POST,
            "/v1/repositories/{repositoryName}/remotes/{remoteName}/commits/{commitId}/push".replace("{"+"repositoryName"+"}", "$repositoryName").replace("{"+"remoteName"+"}", "$remoteName").replace("{"+"commitId"+"}", "$commitId"),
            query = localVariableQuery,
            headers = localVariableHeaders
        )
        val response = request<Operation>(
            localVariableConfig,
            localVariableBody
        )

        return when (response.responseType) {
            ResponseType.Success -> (response as Success<*>).data as Operation
            ResponseType.ClientError -> throw ClientException.fromResponse(gson, response)
            ResponseType.ServerError -> throw ServerException.fromResponse(gson, response)
            else -> throw NotImplementedError(response.responseType.toString())
        }
    }
}
