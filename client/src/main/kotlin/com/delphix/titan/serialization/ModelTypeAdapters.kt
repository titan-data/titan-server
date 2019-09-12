/*
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */

package com.delphix.titan.serialization

import com.delphix.titan.models.EngineRemote
import com.delphix.titan.models.EngineParameters
import com.delphix.titan.models.NopRemote
import com.delphix.titan.models.NopParameters
import com.delphix.titan.models.RemoteParameters
import com.delphix.titan.models.Remote
import com.delphix.titan.models.S3Parameters
import com.delphix.titan.models.S3Remote
import com.delphix.titan.models.SshRemote
import com.delphix.titan.models.SshParameters
import com.google.gson.GsonBuilder

class ModelTypeAdapters {
    companion object {
        private val remote = RuntimeTypeAdapterFactory
                .of(Remote::class.java, "provider", true)
                .registerSubtype(NopRemote::class.java, "nop")
                .registerSubtype(EngineRemote::class.java, "engine")
                .registerSubtype(SshRemote::class.java, "ssh")
                .registerSubtype(S3Remote::class.java, "s3")

        private val request = RuntimeTypeAdapterFactory
                .of(RemoteParameters::class.java, "provider", true)
                .registerSubtype(NopParameters::class.java, "nop")
                .registerSubtype(EngineParameters::class.java, "engine")
                .registerSubtype(SshParameters::class.java, "ssh")
                .registerSubtype(S3Parameters::class.java, "s3")

        fun configure(builder: GsonBuilder) : GsonBuilder {
            return builder.registerTypeAdapterFactory(remote)
                    .registerTypeAdapterFactory(request)
        }
    }
}

