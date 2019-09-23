/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.models

import com.google.gson.annotations.SerializedName

data class VolumeRequest(
    @SerializedName("Name")
    var name: String
)
