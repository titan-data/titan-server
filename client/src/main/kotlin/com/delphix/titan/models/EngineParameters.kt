/*
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */

package com.delphix.titan.models

data class EngineParameters(
    override var provider: String = "engine",
    var password: String? = null
) : RemoteParameters()
