/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.exception

import java.io.IOException

class CommandException(message: String, val exitCode: Int, val output: String) : IOException(message)
