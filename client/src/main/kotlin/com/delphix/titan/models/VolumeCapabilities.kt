/*
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */

package com.delphix.titan.models

import com.google.gson.annotations.SerializedName

data class VolumeCapabilities(
    @SerializedName("Scope")
    var scope: String
)
