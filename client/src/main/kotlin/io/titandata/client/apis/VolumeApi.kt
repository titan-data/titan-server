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
import io.titandata.models.VolumeCapabilitiesResponse
import io.titandata.models.VolumeCreateRequest
import io.titandata.models.VolumeGetResponse
import io.titandata.models.VolumeListResponse
import io.titandata.models.VolumeMountRequest
import io.titandata.models.VolumePathResponse
import io.titandata.models.VolumeRequest
import io.titandata.models.VolumeResponse

class VolumeApi(basePath: String = "http://localhost:5001") : ApiClient(basePath) {

    @Suppress("UNCHECKED_CAST")
    fun createVolume(volumeCreateRequest: VolumeCreateRequest) : VolumeResponse {
        val localVariableBody: Any? = volumeCreateRequest
        val localVariableQuery: Map<String,List<String>> = mapOf()
        val localVariableHeaders: MutableMap<String, String> = mutableMapOf()
        val localVariableConfig = RequestConfig(
                RequestMethod.POST,
                "/VolumeDriver.Create",
                query = localVariableQuery,
                headers = localVariableHeaders
        )
        val response = request<VolumeResponse>(
            localVariableConfig,
            localVariableBody
        )

        return when (response.responseType) {
            ResponseType.Success -> checkResponse((response as Success<*>).data as VolumeResponse)
            ResponseType.ClientError -> throw ClientException.fromResponse(gson, response)
            ResponseType.ServerError -> throw ServerException.fromResponse(gson, response)
            else -> throw NotImplementedError(response.responseType.toString())
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun getCapabilities() : VolumeCapabilitiesResponse {
        val localVariableBody: Any? = null
        val localVariableQuery: Map<String,List<String>> = mapOf()
        val localVariableHeaders: MutableMap<String, String> = mutableMapOf()
        val localVariableConfig = RequestConfig(
                RequestMethod.POST,
                "/VolumeDriver.Capabilities",
                query = localVariableQuery,
                headers = localVariableHeaders
        )
        val response = request<VolumeCapabilitiesResponse>(
            localVariableConfig,
            localVariableBody
        )

        return when (response.responseType) {
            ResponseType.Success -> (response as Success<*>).data as VolumeCapabilitiesResponse
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
    fun getVolume(volumeRequest: VolumeRequest) : VolumeGetResponse {
        val localVariableBody: Any? = volumeRequest
        val localVariableQuery: Map<String,List<String>> = mapOf()
        val localVariableHeaders: MutableMap<String, String> = mutableMapOf()
        val localVariableConfig = RequestConfig(
                RequestMethod.POST,
                "/VolumeDriver.Get",
                query = localVariableQuery,
                headers = localVariableHeaders
        )
        val response = request<VolumeGetResponse>(
            localVariableConfig,
            localVariableBody
        )

        return when (response.responseType) {
            ResponseType.Success -> checkResponse((response as Success<*>).data as VolumeGetResponse)
            ResponseType.ClientError -> throw ClientException.fromResponse(gson, response)
            ResponseType.ServerError -> throw ServerException.fromResponse(gson, response)
            else -> throw NotImplementedError(response.responseType.toString())
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun getVolumePath(volumeRequest: VolumeRequest) : VolumePathResponse {
        val localVariableBody: Any? = volumeRequest
        val localVariableQuery: Map<String,List<String>> = mapOf()
        val localVariableHeaders: MutableMap<String, String> = mutableMapOf()
        val localVariableConfig = RequestConfig(
                RequestMethod.POST,
                "/VolumeDriver.Path",
                query = localVariableQuery,
                headers = localVariableHeaders
        )
        val response = request<VolumePathResponse>(
            localVariableConfig,
            localVariableBody
        )

        return when (response.responseType) {
            ResponseType.Success -> checkResponse((response as Success<*>).data as VolumePathResponse)
            ResponseType.ClientError -> throw ClientException.fromResponse(gson, response)
            ResponseType.ServerError -> throw ServerException.fromResponse(gson, response)
            else -> throw NotImplementedError(response.responseType.toString())
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun listVolumes() : VolumeListResponse {
        val localVariableBody: Any? = null
        val localVariableQuery: Map<String,List<String>> = mapOf()
        val localVariableHeaders: MutableMap<String, String> = mutableMapOf()
        val localVariableConfig = RequestConfig(
                RequestMethod.POST,
                "/VolumeDriver.List",
                query = localVariableQuery,
                headers = localVariableHeaders
        )
        val response = request<VolumeListResponse>(
            localVariableConfig,
            localVariableBody
        )

        return when (response.responseType) {
            ResponseType.Success -> checkResponse((response as Success<*>).data as VolumeListResponse)
            ResponseType.ClientError -> throw ClientException.fromResponse(gson, response)
            ResponseType.ServerError -> throw ServerException.fromResponse(gson, response)
            else -> throw NotImplementedError(response.responseType.toString())
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun mountVolume(volumeMountRequest: VolumeMountRequest) : VolumePathResponse {
        val localVariableBody: Any? = volumeMountRequest
        val localVariableQuery: Map<String,List<String>> = mapOf()
        val localVariableHeaders: MutableMap<String, String> = mutableMapOf()
        val localVariableConfig = RequestConfig(
                RequestMethod.POST,
                "/VolumeDriver.Mount",
                query = localVariableQuery,
                headers = localVariableHeaders
        )
        val response = request<VolumePathResponse>(
            localVariableConfig,
            localVariableBody
        )

        return when (response.responseType) {
            ResponseType.Success -> checkResponse((response as Success<*>).data as VolumePathResponse)
            ResponseType.ClientError -> throw ClientException.fromResponse(gson, response)
            ResponseType.ServerError -> throw ServerException.fromResponse(gson, response)
            else -> throw NotImplementedError(response.responseType.toString())
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun removeVolume(volumeRequest: VolumeRequest) : VolumeResponse {
        val localVariableBody: Any? = volumeRequest
        val localVariableQuery: Map<String,List<String>> = mapOf()
        val localVariableHeaders: MutableMap<String, String> = mutableMapOf()
        val localVariableConfig = RequestConfig(
                RequestMethod.POST,
                "/VolumeDriver.Remove",
                query = localVariableQuery,
                headers = localVariableHeaders
        )
        val response = request<VolumeResponse>(
            localVariableConfig,
            localVariableBody
        )

        return when (response.responseType) {
            ResponseType.Success -> checkResponse((response as Success<*>).data as VolumeResponse)
            ResponseType.ClientError -> throw ClientException.fromResponse(gson, response)
            ResponseType.ServerError -> throw ServerException.fromResponse(gson, response)
            else -> throw NotImplementedError(response.responseType.toString())
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun unmountVolume(volumeMountRequest: VolumeMountRequest) : VolumeResponse {
        val localVariableBody: Any? = volumeMountRequest
        val localVariableQuery: Map<String,List<String>> = mapOf()
        val localVariableHeaders: MutableMap<String, String> = mutableMapOf()
        val localVariableConfig = RequestConfig(
                RequestMethod.POST,
                "/VolumeDriver.Unmount",
                query = localVariableQuery,
                headers = localVariableHeaders
        )
        val response = request<VolumeResponse>(
            localVariableConfig,
            localVariableBody
        )

        return when (response.responseType) {
            ResponseType.Success -> checkResponse((response as Success<*>).data as VolumeResponse)
            ResponseType.ClientError -> throw ClientException.fromResponse(gson, response)
            ResponseType.ServerError -> throw ServerException.fromResponse(gson, response)
            else -> throw NotImplementedError(response.responseType.toString())
        }
    }

    fun checkResponse(response: VolumeResponse) : VolumeResponse {
        if (response.err != "") {
            throw ClientException(response.err)
        }
        return response
    }

    fun checkResponse(response: VolumeGetResponse) : VolumeGetResponse {
        if (response.err != "") {
            throw ClientException(response.err)
        }
        return response
    }

    fun checkResponse(response: VolumePathResponse) : VolumePathResponse {
        if (response.err != "") {
            throw ClientException(response.err)
        }
        return response
    }

    fun checkResponse(response: VolumeListResponse) : VolumeListResponse {
        if (response.err != "") {
            throw ClientException(response.err)
        }
        return response
    }
}
