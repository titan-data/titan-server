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
import io.titandata.models.Operation
import io.titandata.models.RemoteParameters
import io.titandata.models.ProgressEntry

class OperationsApi(basePath: String = "http://localhost:5001") : ApiClient(basePath) {

    fun deleteOperation(repositoryName: String, operationId: String) {
        val localVariableBody: Any? = null
        val localVariableQuery: Map<String,List<String>> = mapOf()
        val localVariableHeaders: MutableMap<String, String> = mutableMapOf()
        val localVariableConfig = RequestConfig(
                RequestMethod.DELETE,
                "/v1/repositories/{repositoryName}/operations/{operationId}".replace("{" + "repositoryName" + "}", "$repositoryName").replace("{" + "operationId" + "}", "$operationId"),
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
        val localVariableQuery: Map<String,List<String>> = mapOf()
        val localVariableHeaders: MutableMap<String, String> = mutableMapOf()
        val localVariableConfig = RequestConfig(
                RequestMethod.GET,
                "/v1/repositories/{repositoryName}/operations/{operationId}".replace("{" + "repositoryName" + "}", "$repositoryName").replace("{" + "operationId" + "}", "$operationId"),
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
        val localVariableQuery: Map<String,List<String>> = mapOf()
        val localVariableHeaders: MutableMap<String, String> = mutableMapOf()
        val localVariableConfig = RequestConfig(
                RequestMethod.GET,
                "/v1/repositories/{repositoryName}/operations/{operationId}/progress".replace("{" + "repositoryName" + "}", "$repositoryName").replace("{" + "operationId" + "}", "$operationId"),
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
        val localVariableQuery: Map<String,List<String>> = mapOf()
        val localVariableHeaders: MutableMap<String, String> = mutableMapOf()
        val localVariableConfig = RequestConfig(
                RequestMethod.GET,
                "/v1/repositories/{repositoryName}/operations".replace("{" + "repositoryName" + "}", "$repositoryName"),
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
        val localVariableQuery: Map<String,List<String>> = mapOf()
        val localVariableHeaders: MutableMap<String, String> = mutableMapOf()
        val localVariableConfig = RequestConfig(
                RequestMethod.POST,
                "/v1/repositories/{repositoryName}/remotes/{remoteName}/commits/{commitId}/pull".replace("{" + "repositoryName" + "}", "$repositoryName").replace("{" + "remoteName" + "}", "$remoteName").replace("{" + "commitId" + "}", "$commitId"),
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
        val localVariableQuery: Map<String,List<String>> = mapOf()
        val localVariableHeaders: MutableMap<String, String> = mutableMapOf()
        val localVariableConfig = RequestConfig(
                RequestMethod.POST,
                "/v1/repositories/{repositoryName}/remotes/{remoteName}/commits/{commitId}/push".replace("{" + "repositoryName" + "}", "$repositoryName").replace("{" + "remoteName" + "}", "$remoteName").replace("{" + "commitId" + "}", "$commitId"),
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
