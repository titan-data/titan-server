/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */

package com.delphix.sdk.repos

import com.delphix.sdk.Http as Http
import org.json.JSONObject

/**
 * The generic source environment schema.
 */
class SourceEnvironment (
        var http: Http
) {
    val root: String = "/resources/json/delphix/environment"

    /**
     * Returns the list of all source environments.
     */
    fun list(): JSONObject {
        return http.handleGet("$root")
    }

    /**
     * Retrieve the specified SourceEnvironment object.
     */
    fun read(ref: String): JSONObject {
        return http.handleGet("$root/$ref")
    }

    /**
     * Create a new SourceEnvironment object.
     */
    fun create(payload: com.delphix.sdk.objects.SourceEnvironmentCreateParameters): JSONObject {
        return http.handlePost("$root", payload.toMap())
    }

    /**
     * Update the specified SourceEnvironment object.
     */
    fun update(ref: String, payload: com.delphix.sdk.objects.SourceEnvironment): JSONObject {
        return http.handlePost("$root/$ref", payload.toMap())
    }

    /**
     * Delete the specified SourceEnvironment object.
     */
    fun delete(ref: String): JSONObject {
        return http.handleDelete("$root/$ref")
    }

}
