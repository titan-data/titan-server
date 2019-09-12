/*
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */

package com.delphix.titan.models

data class NopParameters(
    override var provider: String = "nop",
    var delay: Int = 0
) : RemoteParameters()
