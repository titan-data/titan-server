/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.remote.nop

import io.titandata.models.Remote

data class NopRemote(
    override var provider: String = "nop",
    override var name: String
) : Remote()
