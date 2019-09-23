/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.models

import com.google.gson.annotations.SerializedName

data class VolumePathResponse(
    @SerializedName("Err")
    var err: String = "",
    @SerializedName("Mountpoint")
    var mountpoint: String? = null
)
