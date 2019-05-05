/*
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */

package com.delphix.titan.models

data class ProgressEntry(
    var type: Type,
    var message: String? = null,
    var percent: Int? = null
) {
    enum class Type(val value: String){
        MESSAGE("MESSAGE"),
        START("START"),
        PROGRESS("PROGRESS"),
        END("END"),
        ERROR("ERROR"),
        ABORT("ABORTED"),
        FAILED("FAILED"),
        COMPLETE("COMPLETE");
    }
}
