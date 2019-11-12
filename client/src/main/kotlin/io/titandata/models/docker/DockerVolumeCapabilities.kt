/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.models.docker

import com.google.gson.annotations.SerializedName

data class DockerVolumeCapabilities(
    @SerializedName("Scope")
    var scope: String
)
