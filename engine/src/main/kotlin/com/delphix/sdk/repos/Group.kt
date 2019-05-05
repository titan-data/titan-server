/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */

package com.delphix.sdk.repos

import com.delphix.sdk.Http as Http
import org.json.JSONObject

/**
 * Database group.
 */
class Group (
        var http: Http
) {
    val root: String = "/resources/json/delphix/group"

    /**
     * List Group objects on the system.
     */
    fun list(): JSONObject {
        return http.handleGet("$root")
    }

    /**
     * Retrieve the specified Group object.
     */
    fun read(ref: String): JSONObject {
        return http.handleGet("$root/$ref")
    }

    /**
     * Create a new Group object.
     */
    fun create(payload: com.delphix.sdk.objects.Group): JSONObject {
        return http.handlePost("$root", payload.toMap())
    }

    /**
     * Update the specified Group object.
     */
    fun update(ref: String, payload: com.delphix.sdk.objects.Group): JSONObject {
        return http.handlePost("$root/$ref", payload.toMap())
    }

    /**
     * Delete the specified Group object.
     */
    fun delete(ref: String): JSONObject {
        return http.handlePost("$root/$ref", emptyMap<String, Any?>())
    }

}
