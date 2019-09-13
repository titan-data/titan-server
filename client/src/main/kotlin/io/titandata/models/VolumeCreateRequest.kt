/*
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */

package io.titandata.models

import com.google.gson.annotations.SerializedName

data class VolumeCreateRequest(
    @SerializedName("Name")
    var name: String? = null,
    @SerializedName("Opts")
    var opts: Map<String, Any>? = null
)
