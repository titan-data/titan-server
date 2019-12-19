/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.models.docker

import com.google.gson.annotations.SerializedName

data class PluginDescription(
    @SerializedName("Implements")
    var implements: Array<String> = arrayOf("VolumeDriver")
)
