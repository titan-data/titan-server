/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.models

data class SshParameters(
    override var provider: String = "ssh",
    var password: String? = null,
    var key: String? = null
) : RemoteParameters()
