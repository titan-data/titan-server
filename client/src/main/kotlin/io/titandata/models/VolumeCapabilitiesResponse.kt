/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.models

import com.google.gson.annotations.SerializedName

data class VolumeCapabilitiesResponse(
    @SerializedName("Capabilities")
    var capabilities: VolumeCapabilities? = null
)
