/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.serialization

import io.titandata.remote.engine.EngineRemote
import io.titandata.remote.nop.NopRemote
import io.titandata.models.Remote
import io.titandata.remote.s3.S3Remote
import io.titandata.remote.ssh.SshRemote
import com.google.gson.GsonBuilder
import io.titandata.remote.s3web.S3WebRemote

class ModelTypeAdapters {
    companion object {
        private val remote = RuntimeTypeAdapterFactory.of(Remote::class.java, "provider", true)
                .registerSubtype(NopRemote::class.java, "nop")
                .registerSubtype(EngineRemote::class.java, "engine")
                .registerSubtype(SshRemote::class.java, "ssh")
                .registerSubtype(S3Remote::class.java, "s3")
                .registerSubtype(S3WebRemote::class.java, "s3web")

        fun configure(builder: GsonBuilder) : GsonBuilder {
            return builder.registerTypeAdapterFactory(remote)
        }
    }
}

