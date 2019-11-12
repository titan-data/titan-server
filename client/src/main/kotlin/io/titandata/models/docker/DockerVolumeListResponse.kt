/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.models.docker

import com.google.gson.annotations.SerializedName

data class DockerVolumeListResponse(
    @SerializedName("Err")
    var err: String = "",
    @SerializedName("Volumes")
    var volumes: Array<DockerVolume>
)
