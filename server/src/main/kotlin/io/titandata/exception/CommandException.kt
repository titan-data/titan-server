/*
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */

package io.titandata.exception

import java.io.IOException

class CommandException(message: String, val exitCode: Int, val output: String) : IOException(message)
