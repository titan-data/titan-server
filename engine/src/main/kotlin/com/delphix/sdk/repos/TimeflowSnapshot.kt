/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */

package com.delphix.sdk.repos

import com.delphix.sdk.Http as Http
import org.json.JSONObject

/**
 * Snapshot of a point within a TimeFlow that is used as the basis for provisioning.
 */
class TimeflowSnapshot (
        var http: Http
) {
    val root: String = "/resources/json/delphix/snapshot"

    /**
     * Returns a list of snapshots on the system or within a particular object. By default, all snapshots within the domain are listed.
     */
    fun list(): JSONObject {
        return http.handleGet("$root")
    }

    /**
     * Retrieve the specified TimeflowSnapshot object.
     */
    fun read(ref: String): JSONObject {
        return http.handleGet("$root/$ref")
    }

    /**
     * Update the specified TimeflowSnapshot object.
     */
    fun update(ref: String, payload: com.delphix.sdk.objects.TimeflowSnapshot): JSONObject {
        return http.handlePost("$root/$ref", payload.toMap())
    }

    /**
     * Delete the specified TimeflowSnapshot object.
     */
    fun delete(ref: String): JSONObject {
        return http.handlePost("$root/$ref", emptyMap<String, Any?>())
    }

}
