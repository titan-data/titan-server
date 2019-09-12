/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */

package com.delphix.sdk.repos

import com.delphix.sdk.Http as Http
import org.json.JSONObject

/**
 * The representation of a host object.
 */
class Host (
        var http: Http
) {
    val root: String = "/resources/json/delphix/host"

    /**
     * Returns the list of all hosts in the system.
     */
    fun list(): JSONObject {
        return http.handleGet("$root")
    }

    /**
     * Retrieve the specified Host object.
     */
    fun read(ref: String): JSONObject {
        return http.handleGet("$root/$ref")
    }

    /**
     * Update the specified Host object.
     */
    fun update(ref: String, payload: com.delphix.sdk.objects.Host): JSONObject {
        return http.handlePost("$root/$ref", payload.toMap())
    }

}
