/*
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */

package io.titandata.models

import com.google.gson.annotations.SerializedName

data class VolumeListResponse(
    @SerializedName("Err")
    var err: String = "",
    @SerializedName("Volumes")
    var volumes: Array<Volume>
)
