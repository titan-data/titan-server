/*
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */

package io.titandata.models

import com.google.gson.annotations.SerializedName

data class VolumeGetResponse(
    @SerializedName("Err")
    var err: String = "",
    @SerializedName("Volume")
    var volume: Volume
)
