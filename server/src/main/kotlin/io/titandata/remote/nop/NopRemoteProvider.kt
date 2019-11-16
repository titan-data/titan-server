/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.remote.nop

import io.titandata.models.ProgressEntry
import io.titandata.operation.OperationExecutor
import io.titandata.remote.BaseRemoteProvider

/**
 * The nop (No-operation) is a special provider used for internal testing to make it easier to
 * test local workflows without having to mock out an external remote provider. As its name implies,
 * this will simply ignore any operations. Pushing and pulling will always succeed, though listing
 * remotes will always return an empty list.
 */
class NopRemoteProvider : BaseRemoteProvider() {
    override fun startOperation(operation: OperationExecutor): Any? {
        operation.addProgress(ProgressEntry(ProgressEntry.Type.START, "Running operation"))
        val props = operation.params.properties
        if (props.containsKey("delay")) {
            val delay = props.get("delay").toString().toDouble().toInt()
            if (delay != 0) {
                Thread.sleep(delay * 1000L)
            }
        }
        return null
    }

    override fun endOperation(operation: OperationExecutor, data: Any?) {
        operation.addProgress(ProgressEntry(ProgressEntry.Type.END))
    }
}
