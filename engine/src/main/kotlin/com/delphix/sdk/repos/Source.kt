/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */

package com.delphix.sdk.repos

import com.delphix.sdk.Http as Http
import org.json.JSONObject

/**
 * A source represents an external database instance outside the Delphix system.
 */
class Source (
        var http: Http
) {
    val root: String = "/resources/json/delphix/source"

    /**
     * Lists sources on the system.
     */
    fun list(): JSONObject {
        return http.handleGet("$root")
    }

    /**
     * Retrieve the specified Source object.
     */
    fun read(ref: String): JSONObject {
        return http.handleGet("$root/$ref")
    }

    /**
     * Update the specified Source object.
     */
    fun update(ref: String, payload: com.delphix.sdk.objects.Source): JSONObject {
        return http.handlePost("$root/$ref", payload.toMap())
    }

    /**
     * Disables the given source.
     */
    fun disable(ref: String, payload: com.delphix.sdk.objects.SourceDisableParameters): JSONObject {
        return http.handlePost("$root/$ref/disable", payload.toMap())
    }
}
