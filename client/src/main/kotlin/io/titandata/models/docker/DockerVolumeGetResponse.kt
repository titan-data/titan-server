/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.models.docker

import com.google.gson.annotations.SerializedName

data class DockerVolumeGetResponse(
    @SerializedName("Err")
    var err: String = "",
    @SerializedName("Volume")
    var volume: DockerVolume
)
