/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.models

data class EngineParameters(
    override var provider: String = "engine",
    var password: String? = null
) : RemoteParameters()
