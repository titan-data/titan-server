/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.client.infrastructure

import io.titandata.models.Error
import com.google.gson.Gson
import com.google.gson.JsonParser

fun getError(gson: Gson, body: String?) : Error? {
    try {
        val result = gson.fromJson(body, Error::class.java)
        @Suppress("SENSELESS_COMPARISON")
        if (result != null && result.message != null) {
            return result;
        }
    } catch (e: Exception) {
    }

    /*
     * Docker volume errors are a bit strange in that they just return success but include
     * a "Err" field. Try to pull it out here.
     */
    try {
        val element = JsonParser.parseString(body)
        val obj = element.asJsonObject
        if (obj != null && obj.has("Err") && obj.get("Err").asString != "") {
            return Error(message = obj.get("Err").asString)
        }
        return null
    } catch (e: Exception) {
        return null

    }
}

open class ApiException(message: String, val code : String? = null,
                           val details : String? = null) : RuntimeException(message)

open class ClientException(message: String, code : String? = null,
                           details : String? = null) : ApiException(message, code, details) {

    companion object {
        fun fromResponse(gson: Gson, response: ApiInfrastructureResponse<*>) : ClientException {
            val error = response as ClientError<*>
            if (error.body == null) {
                return ClientException("Unknown error")
            }

            val content = error.body as String
            val err = getError(gson, content)
            if (err != null) {
                return ClientException(err.message, err.code, err.details)
            } else {
                return ClientException(content)
            }
        }
    }
}

open class ServerException(message: String, code: String? = null,
        details: String? = null) : ApiException(message, code, details) {
    companion object {
        fun fromResponse(gson: Gson, response: ApiInfrastructureResponse<*>) : ServerException {
            val error = response as ServerError<*>
            if (error.body == null) {
                return ServerException(error.message ?: "Unknown error")
            }

            val content = error.body as String
            val err = getError(gson, content)
            if (err != null) {
                return ServerException(err.message, err.code, err.details)
            } else {
                return ServerException(content)
            }
        }
    }
}
