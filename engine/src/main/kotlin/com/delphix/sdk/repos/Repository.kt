/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */

package com.delphix.sdk.repos

import com.delphix.sdk.Http as Http
import com.delphix.sdk.objects.Repository as RepoObj
import org.json.JSONObject

class Repository (
    var http: Http
) {
    val resource: String = "/resources/json/delphix/repository"

    fun list(): List<RepoObj> {
        var repositories = mutableListOf<RepoObj>()
        val response = http.handleGet(resource).getJSONArray("result")
        for (i in 0 until response.length()) {
            val repository = response.getJSONObject(i);
            repositories.add(RepoObj.fromJson(repository))
        }
        return repositories
    }

    fun get(ref: String): RepoObj {
        val response = http.handleGet("$resource/$ref")
        val repository = RepoObj.fromJson(response.getJSONObject("result"))
        return repository
    }

}
