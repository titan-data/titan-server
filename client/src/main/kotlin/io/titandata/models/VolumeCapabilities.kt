/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.models

import com.google.gson.annotations.SerializedName

data class VolumeCapabilities(
    @SerializedName("Scope")
    var scope: String
)
