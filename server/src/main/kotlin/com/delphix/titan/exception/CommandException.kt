/*
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */

package com.delphix.titan.exception

import java.io.IOException

class CommandException(message: String, val exitCode: Int, val output: String) : IOException(message)
