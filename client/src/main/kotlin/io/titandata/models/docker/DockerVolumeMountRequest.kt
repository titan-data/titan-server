/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.models.docker

import com.google.gson.annotations.SerializedName

data class DockerVolumeMountRequest(
    @SerializedName("Name")
    var name: String? = null,
    @SerializedName("ID")
    var ID: String? = null
)
