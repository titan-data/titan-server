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
import io.titandata.models.Volume

class VolumesApi(basePath: String = "http://localhost:5001") : ApiClient(basePath) {

    fun deleteVolume(repositoryName: String, volumeName: String ) {
        val localVariableBody: Any? = null
        val localVariableQuery: Map<String,List<String>> = mapOf()
        val localVariableHeaders: MutableMap<String, String> = mutableMapOf()
        val localVariableConfig = RequestConfig(
                RequestMethod.DELETE,
                "/v1/repositories/$repositoryName/volumes/$volumeName",
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

    fun getVolume(repositoryName: String, volumeName: String) : Volume {
        val localVariableBody: Any? = null
        val localVariableQuery: Map<String,List<String>> = mapOf()
        val localVariableHeaders: MutableMap<String, String> = mutableMapOf()
        val localVariableConfig = RequestConfig(
                RequestMethod.GET,
                "/v1/repositories/$repositoryName/volumes/$volumeName",
                query = localVariableQuery,
                headers = localVariableHeaders
        )
        val response = request<Volume>(
            localVariableConfig,
            localVariableBody
        )

        return when (response.responseType) {
            ResponseType.Success -> (response as Success<*>).data as Volume
            ResponseType.ClientError -> throw ClientException.fromResponse(gson, response)
            ResponseType.ServerError -> throw ServerException.fromResponse(gson, response)
            else -> throw NotImplementedError(response.responseType.toString())
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun listVolumes(repositoryName: String) : Array<Volume> {
        val localVariableBody: Any? = null
        val localVariableQuery: Map<String,List<String>> = mapOf()
        val localVariableHeaders: MutableMap<String, String> = mutableMapOf()
        val localVariableConfig = RequestConfig(
                RequestMethod.GET,
                "/v1/repositories/$repositoryName/volumes",
                query = localVariableQuery,
                headers = localVariableHeaders
        )
        val response = request<Array<Volume>>(
            localVariableConfig,
            localVariableBody
        )

        return when (response.responseType) {
            ResponseType.Success -> (response as Success<*>).data as Array<Volume>
            ResponseType.ClientError -> throw ClientException.fromResponse(gson, response)
            ResponseType.ServerError -> throw ServerException.fromResponse(gson, response)
            else -> throw NotImplementedError(response.responseType.toString())
        }
    }

    fun createVolume(repositoryName: String, volume: Volume) : Volume {
        val localVariableBody: Any? = volume
        val localVariableQuery: Map<String,List<String>> = mapOf()
        val localVariableHeaders: MutableMap<String, String> = mutableMapOf()
        val localVariableConfig = RequestConfig(
                RequestMethod.POST,
                "/v1/repositories/$repositoryName/volumes",
                query = localVariableQuery,
                headers = localVariableHeaders
        )
        val response = request<Volume>(
            localVariableConfig,
            localVariableBody
        )

        return when (response.responseType) {
            ResponseType.Success -> (response as Success<*>).data as Volume
            ResponseType.ClientError -> throw ClientException.fromResponse(gson, response)
            ResponseType.ServerError -> throw ServerException.fromResponse(gson, response)
            else -> throw NotImplementedError(response.responseType.toString())
        }
    }

    fun activateVolume(repositoryName: String, volumeName: String) {
        val localVariableBody: Any? = null
        val localVariableQuery: Map<String,List<String>> = mapOf()
        val localVariableHeaders: MutableMap<String, String> = mutableMapOf()
        val localVariableConfig = RequestConfig(
                RequestMethod.POST,
                "/v1/repositories/$repositoryName/volumes/$volumeName/activate",
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

    fun deactivateVolume(repositoryName: String, volumeName: String) {
        val localVariableBody: Any? = null
        val localVariableQuery: Map<String,List<String>> = mapOf()
        val localVariableHeaders: MutableMap<String, String> = mutableMapOf()
        val localVariableConfig = RequestConfig(
                RequestMethod.POST,
                "/v1/repositories/$repositoryName/volumes/$volumeName/deactivate",
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
}
