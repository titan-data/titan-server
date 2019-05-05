/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */

package com.delphix.sdk.repos

import com.delphix.sdk.Http as Http
import org.json.JSONObject

/**
 * Describes a Delphix web service session and is the result of an initial handshake.
 */
class APISession (
        var http: Http
) {
    val root: String = "/resources/json/delphix/session"

    /**
     * Returns the settings of the current session, if one has been started.
     */
    fun read(ref: String): JSONObject {
        return http.handleGet("$root/$ref")
    }

    /**
     * Create a new APISession object.
     */
    fun create(payload: com.delphix.sdk.objects.APISession): JSONObject {
        return http.handlePost("$root", payload.toMap())
    }

}
