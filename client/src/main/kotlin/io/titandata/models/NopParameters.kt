/*
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */

package io.titandata.models

data class NopParameters(
    override var provider: String = "nop",
    var delay: Int = 0
) : RemoteParameters()
