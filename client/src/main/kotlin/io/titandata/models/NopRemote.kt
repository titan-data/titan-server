/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.models

data class NopRemote(
    override var provider: String = "nop",
    override var name: String
) : Remote()
