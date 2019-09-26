/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.models

data class NopParameters(
    override var provider: String = "nop",
    var delay: Int = 0
) : RemoteParameters()
