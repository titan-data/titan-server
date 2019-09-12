/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */

package com.delphix.sdk.repos

import com.delphix.sdk.Http as Http
import org.json.JSONObject

/**
 * Represents an action, a permanent record of activity on the server.
 */
class Action (
        var http: Http
) {
    val root: String = "/resources/json/delphix/action"

    /**
     * Retrieve an historical log of actions.
     */
    fun list(): JSONObject {
        return http.handleGet("$root")
    }

    /**
     * Retrieve the specified Action object.
     */
    fun read(ref: String): JSONObject {
        return http.handleGet("$root/$ref")
    }

}
