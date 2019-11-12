/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.models.docker

import com.google.gson.annotations.SerializedName

data class DockerVolumeResponse(
    @SerializedName("Err")
    var err: String = ""
)
