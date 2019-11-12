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
import io.titandata.models.PluginDescription
import io.titandata.models.docker.DockerVolumeCapabilitiesResponse
import io.titandata.models.docker.DockerVolumeCreateRequest
import io.titandata.models.docker.DockerVolumeGetResponse
import io.titandata.models.docker.DockerVolumeListResponse
import io.titandata.models.docker.DockerVolumeMountRequest
import io.titandata.models.docker.DockerVolumePathResponse
import io.titandata.models.docker.DockerVolumeRequest
import io.titandata.models.docker.DockerVolumeResponse

class DockerVolumeApi(basePath: String = "http://localhost:5001") : ApiClient(basePath) {

    @Suppress("UNCHECKED_CAST")
    fun createVolume(volumeCreateRequest: DockerVolumeCreateRequest) : DockerVolumeResponse {
        val localVariableBody: Any? = volumeCreateRequest
        val localVariableQuery: Map<String,List<String>> = mapOf()
        val localVariableHeaders: MutableMap<String, String> = mutableMapOf()
        val localVariableConfig = RequestConfig(
                RequestMethod.POST,
                "/VolumeDriver.Create",
                query = localVariableQuery,
                headers = localVariableHeaders
        )
        val response = request<DockerVolumeResponse>(
            localVariableConfig,
            localVariableBody
        )

        return when (response.responseType) {
            ResponseType.Success -> checkResponse((response as Success<*>).data as DockerVolumeResponse)
            ResponseType.ClientError -> throw ClientException.fromResponse(gson, response)
            ResponseType.ServerError -> throw ServerException.fromResponse(gson, response)
            else -> throw NotImplementedError(response.responseType.toString())
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun getCapabilities() : DockerVolumeCapabilitiesResponse {
        val localVariableBody: Any? = null
        val localVariableQuery: Map<String,List<String>> = mapOf()
        val localVariableHeaders: MutableMap<String, String> = mutableMapOf()
        val localVariableConfig = RequestConfig(
                RequestMethod.POST,
                "/VolumeDriver.Capabilities",
                query = localVariableQuery,
                headers = localVariableHeaders
        )
        val response = request<DockerVolumeCapabilitiesResponse>(
            localVariableConfig,
            localVariableBody
        )

        return when (response.responseType) {
            ResponseType.Success -> (response as Success<*>).data as DockerVolumeCapabilitiesResponse
            ResponseType.ClientError -> throw ClientException.fromResponse(gson, response)
            ResponseType.ServerError -> throw ServerException.fromResponse(gson, response)
            else -> throw NotImplementedError(response.responseType.toString())
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun pluginActivate() : PluginDescription {
        val localVariableBody: Any? = null
        val localVariableQuery: Map<String,List<String>> = mapOf()
        val localVariableHeaders: MutableMap<String, String> = mutableMapOf()
        val localVariableConfig = RequestConfig(
                RequestMethod.POST,
                "/Plugin.Activate",
                query = localVariableQuery,
                headers = localVariableHeaders
        )
        val response = request<PluginDescription>(
                localVariableConfig,
                localVariableBody
        )

        return when (response.responseType) {
            ResponseType.Success -> (response as Success<*>).data as PluginDescription
            ResponseType.ClientError -> throw ClientException.fromResponse(gson, response)
            ResponseType.ServerError -> throw ServerException.fromResponse(gson, response)
            else -> throw NotImplementedError(response.responseType.toString())
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun getVolume(volumeRequest: DockerVolumeRequest) : DockerVolumeGetResponse {
        val localVariableBody: Any? = volumeRequest
        val localVariableQuery: Map<String,List<String>> = mapOf()
        val localVariableHeaders: MutableMap<String, String> = mutableMapOf()
        val localVariableConfig = RequestConfig(
                RequestMethod.POST,
                "/VolumeDriver.Get",
                query = localVariableQuery,
                headers = localVariableHeaders
        )
        val response = request<DockerVolumeGetResponse>(
            localVariableConfig,
            localVariableBody
        )

        return when (response.responseType) {
            ResponseType.Success -> checkResponse((response as Success<*>).data as DockerVolumeGetResponse)
            ResponseType.ClientError -> throw ClientException.fromResponse(gson, response)
            ResponseType.ServerError -> throw ServerException.fromResponse(gson, response)
            else -> throw NotImplementedError(response.responseType.toString())
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun getVolumePath(volumeRequest: DockerVolumeRequest) : DockerVolumePathResponse {
        val localVariableBody: Any? = volumeRequest
        val localVariableQuery: Map<String,List<String>> = mapOf()
        val localVariableHeaders: MutableMap<String, String> = mutableMapOf()
        val localVariableConfig = RequestConfig(
                RequestMethod.POST,
                "/VolumeDriver.Path",
                query = localVariableQuery,
                headers = localVariableHeaders
        )
        val response = request<DockerVolumePathResponse>(
            localVariableConfig,
            localVariableBody
        )

        return when (response.responseType) {
            ResponseType.Success -> checkResponse((response as Success<*>).data as DockerVolumePathResponse)
            ResponseType.ClientError -> throw ClientException.fromResponse(gson, response)
            ResponseType.ServerError -> throw ServerException.fromResponse(gson, response)
            else -> throw NotImplementedError(response.responseType.toString())
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun listVolumes() : DockerVolumeListResponse {
        val localVariableBody: Any? = null
        val localVariableQuery: Map<String,List<String>> = mapOf()
        val localVariableHeaders: MutableMap<String, String> = mutableMapOf()
        val localVariableConfig = RequestConfig(
                RequestMethod.POST,
                "/VolumeDriver.List",
                query = localVariableQuery,
                headers = localVariableHeaders
        )
        val response = request<DockerVolumeListResponse>(
            localVariableConfig,
            localVariableBody
        )

        return when (response.responseType) {
            ResponseType.Success -> checkResponse((response as Success<*>).data as DockerVolumeListResponse)
            ResponseType.ClientError -> throw ClientException.fromResponse(gson, response)
            ResponseType.ServerError -> throw ServerException.fromResponse(gson, response)
            else -> throw NotImplementedError(response.responseType.toString())
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun mountVolume(volumeMountRequest: DockerVolumeMountRequest) : DockerVolumePathResponse {
        val localVariableBody: Any? = volumeMountRequest
        val localVariableQuery: Map<String,List<String>> = mapOf()
        val localVariableHeaders: MutableMap<String, String> = mutableMapOf()
        val localVariableConfig = RequestConfig(
                RequestMethod.POST,
                "/VolumeDriver.Mount",
                query = localVariableQuery,
                headers = localVariableHeaders
        )
        val response = request<DockerVolumePathResponse>(
            localVariableConfig,
            localVariableBody
        )

        return when (response.responseType) {
            ResponseType.Success -> checkResponse((response as Success<*>).data as DockerVolumePathResponse)
            ResponseType.ClientError -> throw ClientException.fromResponse(gson, response)
            ResponseType.ServerError -> throw ServerException.fromResponse(gson, response)
            else -> throw NotImplementedError(response.responseType.toString())
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun removeVolume(volumeRequest: DockerVolumeRequest) : DockerVolumeResponse {
        val localVariableBody: Any? = volumeRequest
        val localVariableQuery: Map<String,List<String>> = mapOf()
        val localVariableHeaders: MutableMap<String, String> = mutableMapOf()
        val localVariableConfig = RequestConfig(
                RequestMethod.POST,
                "/VolumeDriver.Remove",
                query = localVariableQuery,
                headers = localVariableHeaders
        )
        val response = request<DockerVolumeResponse>(
            localVariableConfig,
            localVariableBody
        )

        return when (response.responseType) {
            ResponseType.Success -> checkResponse((response as Success<*>).data as DockerVolumeResponse)
            ResponseType.ClientError -> throw ClientException.fromResponse(gson, response)
            ResponseType.ServerError -> throw ServerException.fromResponse(gson, response)
            else -> throw NotImplementedError(response.responseType.toString())
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun unmountVolume(volumeMountRequest: DockerVolumeMountRequest) : DockerVolumeResponse {
        val localVariableBody: Any? = volumeMountRequest
        val localVariableQuery: Map<String,List<String>> = mapOf()
        val localVariableHeaders: MutableMap<String, String> = mutableMapOf()
        val localVariableConfig = RequestConfig(
                RequestMethod.POST,
                "/VolumeDriver.Unmount",
                query = localVariableQuery,
                headers = localVariableHeaders
        )
        val response = request<DockerVolumeResponse>(
            localVariableConfig,
            localVariableBody
        )

        return when (response.responseType) {
            ResponseType.Success -> checkResponse((response as Success<*>).data as DockerVolumeResponse)
            ResponseType.ClientError -> throw ClientException.fromResponse(gson, response)
            ResponseType.ServerError -> throw ServerException.fromResponse(gson, response)
            else -> throw NotImplementedError(response.responseType.toString())
        }
    }

    fun checkResponse(response: DockerVolumeResponse) : DockerVolumeResponse {
        if (response.err != "") {
            throw ClientException(response.err)
        }
        return response
    }

    fun checkResponse(response: DockerVolumeGetResponse) : DockerVolumeGetResponse {
        if (response.err != "") {
            throw ClientException(response.err)
        }
        return response
    }

    fun checkResponse(response: DockerVolumePathResponse) : DockerVolumePathResponse {
        if (response.err != "") {
            throw ClientException(response.err)
        }
        return response
    }

    fun checkResponse(response: DockerVolumeListResponse) : DockerVolumeListResponse {
        if (response.err != "") {
            throw ClientException(response.err)
        }
        return response
    }
}
