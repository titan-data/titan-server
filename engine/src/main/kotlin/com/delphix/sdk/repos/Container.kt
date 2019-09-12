/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */

package com.delphix.sdk.repos

import com.delphix.sdk.Http
import org.json.JSONObject

/**
 * A container holding data.
 */
class Container (
        var http: Http
) {
    val root: String = "/resources/json/delphix/database"

    /**
     * Returns a list of databases on the system or within a group.
     */
    fun list(): JSONObject {
        return http.handleGet("$root")
    }

    /**
     * Retrieve the specified Container object.
     */
    fun read(ref: String): JSONObject {
        return http.handleGet("$root/$ref")
    }

    /**
     * Update the specified Container object.
     */
    fun update(ref: String, payload: com.delphix.sdk.objects.Container): JSONObject {
        return http.handlePost("$root/$ref", payload.toMap())
    }

    /**
     * Delete the specified Container object.
     */
    fun delete(ref: String, payload: com.delphix.sdk.objects.DeleteParameters): JSONObject {
        return http.handlePost("$root/$ref/delete", payload.toMap())
    }

    /**
     * Provisions the container specified by the provision parameters.
     */
    fun provision(ref: String, payload: com.delphix.sdk.objects.ProvisionParameters): JSONObject {
        return http.handlePost("$root/$ref", payload.toMap())
    }

    /**
     * Performs SnapSync on a database.
     */
    fun sync(ref: String, payload: com.delphix.sdk.objects.SyncParameters): JSONObject {
        return http.handlePost("$root/$ref/sync", payload.toMap())
    }
}
