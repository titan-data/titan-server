/*
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */

package io.titandata.models

data class NopRemote(
    override var provider: String = "nop",
    override var name: String
) : Remote()
