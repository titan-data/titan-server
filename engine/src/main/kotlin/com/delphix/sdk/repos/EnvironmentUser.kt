/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */

package com.delphix.sdk.repos

import com.delphix.sdk.Http as Http
import org.json.JSONObject

/**
 * The representation of an environment user object.
 */
class EnvironmentUser (
        var http: Http
) {
    val root: String = "/resources/json/delphix/environment/user"

    /**
     * Returns the list of all environment users in the system.
     */
    fun list(): JSONObject {
        return http.handleGet("$root")
    }

    /**
     * Retrieve the specified EnvironmentUser object.
     */
    fun read(ref: String): JSONObject {
        return http.handleGet("$root/$ref")
    }

    /**
     * Create a new EnvironmentUser object.
     */
    fun create(payload: com.delphix.sdk.objects.EnvironmentUser): JSONObject {
        return http.handlePost("$root", payload.toMap())
    }

    /**
     * Update the specified EnvironmentUser object.
     */
    fun update(ref: String, payload: com.delphix.sdk.objects.EnvironmentUser): JSONObject {
        return http.handlePost("$root/$ref", payload.toMap())
    }

    /**
     * Delete the specified EnvironmentUser object.
     */
    fun delete(ref: String): JSONObject {
        return http.handlePost("$root/$ref", emptyMap<String, Any?>())
    }

}
