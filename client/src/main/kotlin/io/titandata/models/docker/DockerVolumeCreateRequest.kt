/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.models.docker

import com.google.gson.annotations.SerializedName

data class DockerVolumeCreateRequest(
    @SerializedName("Name")
    var name: String? = null,
    @SerializedName("Opts")
    var opts: Map<String, Any>? = null
)
