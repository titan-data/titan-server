package com.delphix.titan.client.apis

import com.delphix.titan.client.infrastructure.ApiClient
import com.delphix.titan.client.infrastructure.ClientException
import com.delphix.titan.client.infrastructure.MultiValueMap
import com.delphix.titan.client.infrastructure.RequestConfig
import com.delphix.titan.client.infrastructure.RequestMethod
import com.delphix.titan.client.infrastructure.ResponseType
import com.delphix.titan.client.infrastructure.ServerException
import com.delphix.titan.client.infrastructure.Success
import com.delphix.titan.models.PluginDescription
import com.delphix.titan.models.VolumeCapabilitiesResponse
import com.delphix.titan.models.VolumeCreateRequest
import com.delphix.titan.models.VolumeGetResponse
import com.delphix.titan.models.VolumeListResponse
import com.delphix.titan.models.VolumeMountRequest
import com.delphix.titan.models.VolumePathResponse
import com.delphix.titan.models.VolumeRequest
import com.delphix.titan.models.VolumeResponse

class VolumeApi(basePath: String = "http://localhost:5001") : ApiClient(basePath) {

    @Suppress("UNCHECKED_CAST")
    fun createVolume(volumeCreateRequest: VolumeCreateRequest) : VolumeResponse {
        val localVariableBody: Any? = volumeCreateRequest
        val localVariableQuery: MultiValueMap = mapOf()
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
        val localVariableQuery: MultiValueMap = mapOf()
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
        val localVariableQuery: MultiValueMap = mapOf()
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
        val localVariableQuery: MultiValueMap = mapOf()
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
        val localVariableQuery: MultiValueMap = mapOf()
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
        val localVariableQuery: MultiValueMap = mapOf()
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
        val localVariableQuery: MultiValueMap = mapOf()
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
        val localVariableQuery: MultiValueMap = mapOf()
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
        val localVariableQuery: MultiValueMap = mapOf()
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
