/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */

package com.delphix.sdk

import okhttp3.*
import okhttp3.MediaType.Companion.toMediaTypeOrNull
import okhttp3.RequestBody.Companion.toRequestBody
import org.json.JSONObject
import java.io.IOException
import java.util.concurrent.TimeUnit

class Http(
        val engineAddress: String,
        private val debug: Boolean = false,
        private val versionMajor: Int = 1,
        private val versionMinor: Int = 7,
        private val versionMicro: Int = 0,
        private val timeout: Long = 60,
        private val timeoutUnit: TimeUnit = TimeUnit.MINUTES
){
    private val sessionResource: String = "/resources/json/delphix/session"
    private var JSESSIONID: String = ""

    private fun call(request: Request): ResponseBody {
        val caller = OkHttpClient.Builder()
                .readTimeout(timeout, timeoutUnit)
                .build()
        val response = caller.newCall(request).execute()
        if (!response.isSuccessful) {
            throw IOException("Unexpected Code: $response")
        }
        checkCookie(response)
        return response.body!!
    }

    private fun validateResponse(response: JSONObject) {
        if (debug) println(response)
        if (response.get("status") == "ERROR") {
            val error = response.getJSONObject("error")
            val details = error.get("details")
            val action = error.get("action")
            throw Exception("$details $action")
        }
    }

    private fun requestSessions(): Map<String, Any> {
        val version = mapOf("type" to "APIVersion", "major" to versionMajor, "minor" to versionMinor, "micro" to versionMicro)
        return mapOf("type" to "APISession", "version" to version)
    }

    private fun checkCookie(r: Response) {
        val cookieDough = r.header("Set-Cookie")
        if (!cookieDough.isNullOrEmpty()) {
            val cookies = cookieDough.split(";")
            JSESSIONID = cookies[0].split("=")[1]
        }
    }

    fun setSession() {
        val json = "application/json; charset=utf-8".toMediaTypeOrNull()
        val requestBody = JSONObject(requestSessions()).toString().toRequestBody(json)
        val request = Request.Builder()
                .url("$engineAddress$sessionResource")
                .post(requestBody)
                .build()
        val caller = OkHttpClient.Builder()
                .readTimeout(timeout, timeoutUnit)
                .build()
        val response = caller.newCall(request).execute()
        checkCookie(response)
    }

    fun handleGet(url: String): JSONObject {
        if (debug) println(url)
        val request = Request.Builder()
                .url("$engineAddress$url")
                .addHeader("Cookie","JSESSIONID=$JSESSIONID")
                .build()
        val response = call(request).asJsonObject()
        validateResponse(response)
        return response
    }

    fun handlePost(url: String, data: Map<String, Any?>): JSONObject {
        if (debug) println(url)
        val json = "application/json; charset=utf-8".toMediaTypeOrNull()
        val requestBody = JSONObject(data).toString().toRequestBody(json)
        val request = Request.Builder()
                .url("$engineAddress$url")
                .addHeader("Cookie","JSESSIONID=$JSESSIONID")
                .post(requestBody)
                .build()
        val response = call(request).asJsonObject()
        validateResponse(response)
        return response
    }

    fun handleDelete(url:String): JSONObject {
        if (debug) println(url)
        val request = Request.Builder()
                .url("$engineAddress$url")
                .addHeader("Cookie","JSESSIONID=$JSESSIONID")
                .delete()
                .build()
        val response = call(request).asJsonObject()
        validateResponse(response)
        return response
    }

    companion object {
        fun ResponseBody.asString(): String {
            return this.string()
        }
        fun ResponseBody.asJsonObject(): JSONObject {
            return JSONObject(this.asString())
        }
    }
}
