/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.remote.engine

import io.titandata.models.RemoteParameters

data class EngineParameters(
    override var provider: String = "engine",
    var password: String? = null
) : RemoteParameters()
