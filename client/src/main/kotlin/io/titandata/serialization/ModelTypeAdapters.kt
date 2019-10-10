/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.serialization

import io.titandata.remote.engine.EngineRemote
import io.titandata.remote.engine.EngineParameters
import io.titandata.remote.nop.NopRemote
import io.titandata.remote.nop.NopParameters
import io.titandata.models.RemoteParameters
import io.titandata.models.Remote
import io.titandata.remote.s3.S3Parameters
import io.titandata.remote.s3.S3Remote
import io.titandata.remote.ssh.SshRemote
import io.titandata.remote.ssh.SshParameters
import com.google.gson.GsonBuilder

class ModelTypeAdapters {
    companion object {
        private val remote = RuntimeTypeAdapterFactory.of(Remote::class.java, "provider", true)
                .registerSubtype(NopRemote::class.java, "nop")
                .registerSubtype(EngineRemote::class.java, "engine")
                .registerSubtype(SshRemote::class.java, "ssh")
                .registerSubtype(S3Remote::class.java, "s3")

        private val request = RuntimeTypeAdapterFactory.of(RemoteParameters::class.java, "provider", true)
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

