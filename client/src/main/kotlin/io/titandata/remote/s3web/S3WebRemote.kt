/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.remote.s3web

import io.titandata.models.Remote

data class S3WebRemote(
    override var provider: String = "s3web",
    override var name: String,
    var url: String
) : Remote()
