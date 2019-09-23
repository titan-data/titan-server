/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.models

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
