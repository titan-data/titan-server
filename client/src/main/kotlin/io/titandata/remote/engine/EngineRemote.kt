/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.remote.engine

import io.titandata.models.Remote

data class EngineRemote(
    override var provider: String = "engine",
    override var name: String,
    var address: String,
    var username: String,
    var password: String? = null,
    var repository: String
) : Remote()
