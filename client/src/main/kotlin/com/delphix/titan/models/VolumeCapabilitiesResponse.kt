/*
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */

package com.delphix.titan.models

import com.google.gson.annotations.SerializedName

data class VolumeCapabilitiesResponse(
    @SerializedName("Capabilities")
    var capabilities: VolumeCapabilities? = null
)
