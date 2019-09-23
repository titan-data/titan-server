/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.util

import io.titandata.exception.CommandException
import java.io.IOException
import java.util.concurrent.TimeUnit
import org.slf4j.LoggerFactory

/**
 * Handle invocation of external commands. This is a wrapper around the native interfaces that
 * handles all of the stdin/stdout errors, and will throw an exception if the command returns
 * a non-zero exit status. It is also a convenient to mock out any dependencies on the external
 * system.
 */
class CommandExecutor(val timeout: Long = 60) {

    companion object {
        val log = LoggerFactory.getLogger(CommandExecutor::class.java)
    }

    fun exec(process: Process, argString: String): String {
        try {
            val output = getOutput(process)
            process.waitFor(timeout, TimeUnit.SECONDS)
            if (process.isAlive) {
                log.error("Timeout: $argString")
                throw IOException("Timed out waiting for command: $argString")
            }
            checkResult(process)
            log.debug("Success: $argString")
            return output
        } catch (e: CommandException) {
            log.error("Exit ${process.exitValue()}: $argString")
            throw e
        } finally {
            process.destroy()
        }
    }

    /**
     * Execute the given command. Throws an exception if the command fails. This is designed for
     * short-lived commands, and has a sixty second timeout just in case. If you want to do
     * anything more complex, use the start() method instead.
     */
    fun exec(vararg args: String): String {
        val process = start(*args)
        val argString = args.joinToString()
        return exec(process, argString)
    }

    /**
     * This method provides back the raw process, which then can be used to read or write
     * data to the input stream, etc.
     */
    fun start(vararg args: String): Process {
        return ProcessBuilder().command(*args).start()
    }

    /**
     * Checks whether the command succeeded, throwing an exception with an appropriate error
     * message if that's not the case.
     */
    fun checkResult(process: Process) {
        if (process.exitValue() != 0) {
            val errOutput = process.errorStream.bufferedReader().readText()
            throw CommandException("Command failed: $errOutput",
                    exitCode = process.exitValue(),
                    output = errOutput)
        }
    }

    /**
     * Fetches the output of the given process and returns it.
     */
    fun getOutput(process: Process): String {
        return process.inputStream.bufferedReader().readText()
    }
}
