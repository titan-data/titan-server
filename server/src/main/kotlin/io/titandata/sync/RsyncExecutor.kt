/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.sync

import io.titandata.exception.CommandException
import io.titandata.exception.InvalidStateException
import io.titandata.models.ProgressEntry
import io.titandata.operation.OperationExecutor
import io.titandata.util.CommandExecutor
import java.io.File
import java.io.InputStream
import java.math.BigDecimal
import java.math.MathContext
import java.nio.file.Files
import java.nio.file.attribute.PosixFilePermission
import org.slf4j.LoggerFactory

/**
 * This is a wrapper around the rsync command line tool, feeding progress back into the executor.
 * It is designed to work with any kind of remote SSH server, including engine remotes and SSH
 * remotes.
 *
 * The username and and host should be encoded into either the src
 */
class RsyncExecutor(
    val executor: OperationExecutor,
    val port: Int?,
    val password: String?,
    val key: String?,
    val src: String,
    val dst: String,
    val cmd: CommandExecutor = CommandExecutor()
) : Runnable {

    companion object {
        val log = LoggerFactory.getLogger(CommandExecutor::class.java)
    }

    private var tmpfile: File? = null

    /*
     * Regular expression to match rsync progress. The rsync source code looks like the following:
     *
     *         ("\r%15s %3d%% %7.2f%s %s%s", human_num(ofs), pct, rate, units, rembuf, eol)
     *
     * Because we run with --no-human-readable, human_num() just prints the digits. While "units"
     * is one of "GB/s", "MB/s", or "kB/s". These numbers are left-padded with spaces, so we allow
     * for any number of spaces in between.
     */
    private val progressRegex = Regex("^\\s*(\\d+)\\s+(\\d+)%\\s+([\\d\\.]+)([GMk])B/s")

    /*
     * Regular expression for the completion line of the rsync process. The source code prints
     * this as:
     *
     *       "sent %s bytes  received %s bytes  %s bytes/sec\n",
     *              human_num(total_written), human_num(total_read), bytes_per_sec_human_dnum())
     *
     * Since we print with --no-human-readable, this is really just two longs (sent/received), and
     * a floating point number for the rate.
     */
    private val completionRegex = Regex("^sent\\s+(\\d+).*\\s+(\\d+) bytes\\s+([\\d\\.]+) bytes/sec")

    fun buildSshCommand(
        file: File,
        vararg command: String
    ): List<String> {
        val args = mutableListOf<String>()

        if (password != null && key != null) {
            throw IllegalArgumentException("only one of password or key can be specified")
        } else if (password != null) {
            file.writeText(password)
            args.addAll(arrayOf("sshpass", "-f", file.path, "ssh"))
        } else if (key != null) {
            file.writeText(key)
            args.addAll(arrayOf("ssh", "-i", file.path))
        } else {
            throw IllegalArgumentException("one of password or key must be specified")
        }
        Files.setPosixFilePermissions(file.toPath(), mutableSetOf(
                PosixFilePermission.OWNER_READ
        ))

        if (port != null) {
            args.addAll(arrayOf("-p", port.toString()))
        }

        args.addAll(arrayOf("-o", "StrictHostKeyChecking=no"))
        args.addAll(command)

        return args
    }

    override fun run() {
        val file = createTempFile()
        file.deleteOnExit()
        try {
            /*
             * Assuming that the remote and parameters are both from the SSH provider is not a
             * valid assumption. We will have to revisit this when we have add a second provider
             * that re-uses the rsync executor.
             */
            val sshCommand = buildSshCommand(file)
            val args = arrayOf("rsync", "-e", sshCommand.joinToString(" "),
                    "--info=progress2,stats2", "--inplace", "--delete", "--partial",
                    "-raz", "--rsync-path=sudo rsync", "--no-human-readable", src, dst)
            log.info("running command: ${args.joinToString()}")
            val process = cmd.start(*args)
            try {
                val stream = process.inputStream
                val endSeen = processOutput(stream)

                // If we've consumed all stdout, the process should be done but wait to be sure
                process.waitFor()

                // Check exit code and throw error if necessary
                cmd.checkResult(process)

                // If we get here, something has gone wrong - the command succeeded but we didn't
                // see the expected termination line.
                if (!endSeen) {
                    log.error("rsync succeeded, but no summary statistics found")
                    throw Exception("rsync command succeeded, but failed to find summary statistics")
                }
                log.info("rsync succeeded")
            } catch (e: CommandException) {
                log.error("rsync command failed: ${e.output}")
                throw e
            } finally {
                process.destroy()
                file.delete()
            }
        } finally {
            cleanup()
        }
    }

    fun processOutput(output: InputStream): Boolean {
        val reader = output.bufferedReader()
        var endSeen = false
        while (true) {
            val line = reader.readLine()
            if (line == null) {
                return endSeen
            }

            endSeen = endSeen || processLine(line.trim())
        }
    }

    /**
     * We run rsync with the argument "--info=progress2,stats2". This will create three bodies of
     * output:
     *
     *           28045990  99%   15.57MB/s    0:00:01 (xfr#203, to-chk=0/301)
     *      ...
     *
     *      Number of files: 301 (reg: 203, dir: 29, link: 69)
     *      Number of created files: 301 (reg: 203, dir: 29, link: 69)
     *      ...
     *
     *      sent 11158327 bytes  received 4227 bytes  4465021.60 bytes/sec
     *      total size is 28047108  speedup is 2.51
     *
     * To process this, our strategy is:
     *
     *      * If the line is blank, ignore it
     *      * If this is a progress line, generate and add a ProgressEntry record
     *      * Log the output
     *      * If this is one of the summary lines, generate an END record with the summary
     *
     * This returns true if the line is the completion line, false otherwise.
     */
    fun processLine(line: String): Boolean {
        if (line == "") {
            return false
        }

        val progressMatch = progressRegex.find(line)
        if (progressMatch != null) {
            val sent = progressMatch.groupValues[1].toLong()
            val percent = progressMatch.groupValues[2].toInt()
            val rawRate = progressMatch.groupValues[3].toDouble()
            val rate = when (progressMatch.groupValues[4]) {
                "G" -> rawRate * 1024 * 1024 * 1024
                "M" -> rawRate * 1024 * 1024
                "k" -> rawRate * 1024
                else -> throw InvalidStateException("unknown rsync units '${progressMatch.groupValues[4]}'")
            }

            executor.addProgress(ProgressEntry(type = ProgressEntry.Type.PROGRESS, percent = percent,
                    message = "${numberToString(sent.toDouble())}B (${numberToString(rate)}B/s)"))
            return false
        }

        log.debug(line)

        val completionMatch = completionRegex.find(line)
        if (completionMatch != null) {
            val sent = completionMatch.groupValues[1].toLong()
            val received = completionMatch.groupValues[2].toLong()
            val rate = completionMatch.groupValues[3].toDouble()

            val sentStr = numberToString(sent.toDouble()) + "B"
            val receivedStr = numberToString(received.toDouble()) + "B"
            val rateStr = numberToString(rate) + "B/sec"

            executor.addProgress(ProgressEntry(type = ProgressEntry.Type.END,
                    message = "$sentStr sent  $receivedStr received  ($rateStr)"))
            return true
        }

        return false
    }

    fun cleanup() {
        if (tmpfile != null) {
            tmpfile!!.delete()
            tmpfile = null
        }
    }

    /*
     * This is a pretty printing function for numbers with bases, modeled after ZFS behavior. It
     * essentially takes converts it to a unit scale and prints it with four digits of precision.
     * Technically, this should be "Ki" etc since we're using base 1024, but despite being
     * correct it's more confusing to the average user.
     */
    fun numberToString(rawValue: Double): String {

        val suffixes = arrayOf("", "K", "M", "G", "T", "P", "E")
        var suffix = ""
        var value = rawValue
        for (s in suffixes) {
            if (value < 1024) {
                suffix = s
                break
            }
            value /= 1024
        }

        var valueStr: String
        if (value > 1024 || Math.round(value).toDouble() == value)
            valueStr = Math.round(value).toString()
        else if (value < 10)
            valueStr = "%.2f".format(value)
        else
            valueStr = BigDecimal(value, MathContext(4)).toString()

        return valueStr + suffix
    }
}
