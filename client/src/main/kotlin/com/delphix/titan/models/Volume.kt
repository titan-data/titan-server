/*
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */

package com.delphix.titan.models

import com.google.gson.annotations.SerializedName

data class Volume(
    @SerializedName("Name")
    var name: String,
    @SerializedName("Mountpoint")
    var mountpoint: String? = null,
    @SerializedName("Status")
    var status: Any? = null,
    var properties: Map<String, Any>? = null
)
