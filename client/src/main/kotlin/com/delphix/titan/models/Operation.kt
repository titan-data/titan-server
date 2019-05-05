/*
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */

package com.delphix.titan.models

data class Operation(
    var id: String,
    var type: Type,
    var state: State = State.RUNNING,
    var remote: String,
    var commitId: String
) {

    enum class Type(val value: String) {
        PUSH("PUSH"),
        PULL("PULL");
    }

    enum class State(val value: String) {
        RUNNING("RUNNING"),
        ABORTED("ABORTED"),
        FAILED("FAILED"),
        COMPLETE("COMPLETE");
    }
}
