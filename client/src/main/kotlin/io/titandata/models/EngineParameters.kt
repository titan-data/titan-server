/*
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */

package io.titandata.models

data class EngineParameters(
    override var provider: String = "engine",
    var password: String? = null
) : RemoteParameters()
