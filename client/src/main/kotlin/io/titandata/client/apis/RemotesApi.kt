package io.titandata.client.apis

import io.titandata.client.infrastructure.ApiClient
import io.titandata.client.infrastructure.ClientException
import io.titandata.client.infrastructure.MultiValueMap
import io.titandata.client.infrastructure.RequestConfig
import io.titandata.client.infrastructure.RequestMethod
import io.titandata.client.infrastructure.ResponseType
import io.titandata.client.infrastructure.ServerException
import io.titandata.client.infrastructure.Success
import io.titandata.models.Commit
import io.titandata.models.Remote
import io.titandata.models.RemoteParameters

class RemotesApi(basePath: String = "http://localhost:5001") : ApiClient(basePath) {

    fun deleteRemote(repositoryName: String, remoteName: String ) {
        val localVariableBody: Any? = null
        val localVariableQuery: MultiValueMap = mapOf()
        val localVariableHeaders: MutableMap<String, String> = mutableMapOf()
        val localVariableConfig = RequestConfig(
                RequestMethod.DELETE,
                "/v1/repositories/{repositoryName}/remotes/{remoteName}".replace("{" + "repositoryName" + "}", "$repositoryName").replace("{" + "remoteName" + "}", "$remoteName"),
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
    fun getRemote(repositoryName: String, remoteName: String) : Remote {
        val localVariableBody: Any? = null
        val localVariableQuery: MultiValueMap = mapOf()
        val localVariableHeaders: MutableMap<String, String> = mutableMapOf()
        val localVariableConfig = RequestConfig(
                RequestMethod.GET,
                "/v1/repositories/{repositoryName}/remotes/{remoteName}".replace("{" + "repositoryName" + "}", "$repositoryName").replace("{" + "remoteName" + "}", "$remoteName"),
                query = localVariableQuery,
                headers = localVariableHeaders
        )
        val response = request<Remote>(
            localVariableConfig,
            localVariableBody
        )

        return when (response.responseType) {
            ResponseType.Success -> (response as Success<*>).data as Remote
            ResponseType.ClientError -> throw ClientException.fromResponse(gson, response)
            ResponseType.ServerError -> throw ServerException.fromResponse(gson, response)
            else -> throw NotImplementedError(response.responseType.toString())
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun getRemoteCommit(repositoryName: String, remoteName: String, commitId: String, params: RemoteParameters) : Commit {
        val localVariableBody: Any? = null
        val localVariableQuery: MultiValueMap = mapOf()
        val localVariableHeaders: MutableMap<String, String> = mutableMapOf(
                "titan-remote-parameters" to gson.toJson(params)
        )
        val localVariableConfig = RequestConfig(
                RequestMethod.GET,
                "/v1/repositories/{repositoryName}/remotes/{remoteName}/commits/{commitId}".replace("{" + "repositoryName" + "}", "$repositoryName").replace("{" + "remoteName" + "}", "$remoteName").replace("{" + "commitId" + "}", "$commitId"),
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
    fun listRemoteCommits(repositoryName: String, remoteName: String, params: RemoteParameters) : Array<Commit> {
        val localVariableBody: Any? = null
        val localVariableQuery: MultiValueMap = mapOf()
        val localVariableHeaders: MutableMap<String, String> = mutableMapOf(
            "titan-remote-parameters" to gson.toJson(params)
        )
        val localVariableConfig = RequestConfig(
                RequestMethod.GET,
                "/v1/repositories/{repositoryName}/remotes/{remoteName}/commits".replace("{" + "repositoryName" + "}", "$repositoryName").replace("{" + "remoteName" + "}", "$remoteName"),
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

    @Suppress("UNCHECKED_CAST")
    fun listRemotes(repositoryName: String) : Array<Remote> {
        val localVariableBody: Any? = null
        val localVariableQuery: MultiValueMap = mapOf()
        val localVariableHeaders: MutableMap<String, String> = mutableMapOf()
        val localVariableConfig = RequestConfig(
                RequestMethod.GET,
                "/v1/repositories/{repositoryName}/remotes".replace("{" + "repositoryName" + "}", "$repositoryName"),
                query = localVariableQuery,
                headers = localVariableHeaders
        )
        val response = request<Array<Remote>>(
            localVariableConfig,
            localVariableBody
        )

        return when (response.responseType) {
            ResponseType.Success -> (response as Success<*>).data as Array<Remote>
            ResponseType.ClientError -> throw ClientException.fromResponse(gson, response)
            ResponseType.ServerError -> throw ServerException.fromResponse(gson, response)
            else -> throw NotImplementedError(response.responseType.toString())
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun createRemote(repositoryName: String, anyRemote: Remote) : Remote {
        val localVariableBody: Any? = anyRemote
        val localVariableQuery: MultiValueMap = mapOf()
        val localVariableHeaders: MutableMap<String, String> = mutableMapOf()
        val localVariableConfig = RequestConfig(
                RequestMethod.POST,
                "/v1/repositories/{repositoryName}/remotes".replace("{" + "repositoryName" + "}", "$repositoryName"),
                query = localVariableQuery,
                headers = localVariableHeaders
        )
        val response = request<Remote>(
            localVariableConfig,
            localVariableBody
        )

        return when (response.responseType) {
            ResponseType.Success -> (response as Success<*>).data as Remote
            ResponseType.ClientError -> throw ClientException.fromResponse(gson, response)
            ResponseType.ServerError -> throw ServerException.fromResponse(gson, response)
            else -> throw NotImplementedError(response.responseType.toString())
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun updateRemote(repositoryName: String, remoteName: String, anyRemote: Remote) : Remote {
        val localVariableBody: Any? = anyRemote
        val localVariableQuery: MultiValueMap = mapOf()
        val localVariableHeaders: MutableMap<String, String> = mutableMapOf()
        val localVariableConfig = RequestConfig(
                RequestMethod.POST,
                "/v1/repositories/{repositoryName}/remotes/{remoteName}".replace("{" + "repositoryName" + "}", "$repositoryName").replace("{" + "remoteName" + "}", "$remoteName"),
                query = localVariableQuery,
                headers = localVariableHeaders
        )
        val response = request<Remote>(
            localVariableConfig,
            localVariableBody
        )

        return when (response.responseType) {
            ResponseType.Success -> (response as Success<*>).data as Remote
            ResponseType.ClientError -> throw ClientException.fromResponse(gson, response)
            ResponseType.ServerError -> throw ServerException.fromResponse(gson, response)
            else -> throw NotImplementedError(response.responseType.toString())
        }
    }
}
