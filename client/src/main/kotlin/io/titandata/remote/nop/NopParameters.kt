/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.remote.nop

import io.titandata.models.RemoteParameters

data class NopParameters(
    override var provider: String = "nop",
    var delay: Int = 0
) : RemoteParameters()
