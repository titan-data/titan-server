/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.models

import com.google.gson.annotations.SerializedName

data class VolumeResponse(
    @SerializedName("Err")
    var err: String = ""
)
