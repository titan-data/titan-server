/*
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */

package io.titandata.models

data class SshParameters(
    override var provider: String = "ssh",
    var password: String? = null,
    var key: String? = null
) : RemoteParameters()
