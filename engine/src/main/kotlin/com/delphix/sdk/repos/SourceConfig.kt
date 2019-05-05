/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */

package com.delphix.sdk.repos

import com.delphix.sdk.Http as Http
import org.json.JSONObject

/**
 * The source config represents the dynamically discovered attributes of a source.
 */
class SourceConfig (
        var http: Http
) {
    val root: String = "/resources/json/delphix/sourceconfig"

    /**
     * Returns a list of source configs within the repository or the environment.
     */
    fun list(): JSONObject {
        return http.handleGet("$root")
    }

    /**
     * Retrieve the specified SourceConfig object.
     */
    fun read(ref: String): JSONObject {
        return http.handleGet("$root/$ref")
    }

    /**
     * Create a new SourceConfig object.
     */
    fun create(payload: com.delphix.sdk.objects.SourceConfig): JSONObject {
        return http.handlePost("$root", payload.toMap())
    }

    /**
     * Update the specified SourceConfig object.
     */
    fun update(ref: String, payload: com.delphix.sdk.objects.SourceConfig): JSONObject {
        return http.handlePost("$root/$ref", payload.toMap())
    }

    /**
     * Delete the specified SourceConfig object.
     */
    fun delete(ref: String): JSONObject {
        return http.handlePost("$root/$ref", emptyMap<String, Any?>())
    }

}
