/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.models.docker

import com.google.gson.annotations.SerializedName

data class DockerVolumePathResponse(
    @SerializedName("Err")
    var err: String = "",
    @SerializedName("Mountpoint")
    var mountpoint: String? = null
)
