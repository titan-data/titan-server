/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.models

data class SshRemote(
    override var provider: String = "ssh",
    override var name: String,
    var address: String,
    var username: String,
    var password: String? = null,
    var keyFile: String? = null,
    var port: Int? = null,
    var path: String
) : Remote()
