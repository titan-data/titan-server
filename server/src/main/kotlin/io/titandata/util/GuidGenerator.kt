/*
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */

package io.titandata.util

import java.util.UUID

/**
 * This is simple class that will generate random UUIDs for operations, commits, and other internal
 * uses. We create this utility class primarily to aid in testing, as we can easily mock it out and
 * control the generated IDs.
 */
class GuidGenerator {

    fun get(): String {
        return UUID.randomUUID().toString()
    }
}
