/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.models

import com.google.gson.annotations.SerializedName

data class VolumeGetResponse(
    @SerializedName("Err")
    var err: String = "",
    @SerializedName("Volume")
    var volume: Volume
)
