package com.delphix.titan

import com.delphix.titan.apis.CommitsApi
import com.delphix.titan.apis.OperationsApi
import com.delphix.titan.apis.RemotesApi
import com.delphix.titan.apis.RepositoriesApi
import com.delphix.titan.apis.VolumeApi
import com.delphix.titan.exception.NoSuchObjectException
import com.delphix.titan.exception.ObjectExistsException
import io.titandata.models.Error
import io.titandata.models.VolumeResponse
import com.delphix.titan.operation.OperationProvider
import com.delphix.titan.remote.RemoteProvider
import com.delphix.titan.remote.engine.EngineRemoteProvider
import com.delphix.titan.remote.nop.NopRemoteProvider
import com.delphix.titan.remote.s3.S3RemoteProvider
import com.delphix.titan.remote.ssh.SshRemoteProvider
import io.titandata.serialization.ModelTypeAdapters
import com.delphix.titan.storage.StorageProvider
import com.delphix.titan.storage.zfs.ZfsStorageProvider
import com.delphix.titan.util.CommandExecutor
import com.google.gson.GsonBuilder
import com.google.gson.JsonSyntaxException
import io.ktor.application.Application
import io.ktor.application.call
import io.ktor.application.install
import io.ktor.application.log
import io.ktor.features.CallLogging
import io.ktor.features.Compression
import io.ktor.features.ContentNegotiation
import io.ktor.features.DefaultHeaders
import io.ktor.features.StatusPages
import io.ktor.features.deflate
import io.ktor.features.gzip
import io.ktor.features.minimumSize
import io.ktor.gson.GsonConverter
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.request.ApplicationRequest
import io.ktor.request.path
import io.ktor.response.respond
import io.ktor.routing.Routing
import io.ktor.server.cio.CIO
import io.ktor.server.engine.embeddedServer
import io.ktor.util.KtorExperimentalAPI
import java.io.PrintWriter
import java.io.StringWriter
import org.slf4j.event.Level

fun exceptionToError(request: ApplicationRequest, t: Throwable): Any {
    if (request.path().startsWith("/VolumeDriver.")) {
        return VolumeResponse(err = t.message ?: "unknown error")
    } else {
        val sw = StringWriter()
        t.printStackTrace(PrintWriter(sw))
        return Error(
                code = t.javaClass.simpleName,
                message = t.message ?: "unknown error",
                details = sw.toString()
        )
    }
}

class ProviderModule(val pool: String) {
    private val zfsStorageProvider = ZfsStorageProvider(pool)
    private val nopRemoteProvider = NopRemoteProvider()
    private val engineRemoteProvider = EngineRemoteProvider(this)
    private val sshRemoteProvider = SshRemoteProvider(this)
    private val operationProvider = OperationProvider(this)
    private val s3Provider = S3RemoteProvider(this)

    val gson = ModelTypeAdapters.configure(GsonBuilder()).create()
    val commandExecutor = CommandExecutor()

    // Return the default storage provider
    val storage: StorageProvider
        get() = zfsStorageProvider

    // Return the operation provider
    val operation: OperationProvider
        get() = operationProvider

    // Get a storage provider by name (only ZFS is supported)
    fun storage(type: String): StorageProvider {
        if (type != "zfs") {
            throw IllegalArgumentException("unknown storage provider '$type'")
        }
        return zfsStorageProvider
    }

    // Get a remote provider by name
    fun remote(type: String): RemoteProvider {
        return when (type) {
            "nop" -> nopRemoteProvider
            "engine" -> engineRemoteProvider
            "ssh" -> sshRemoteProvider
            "s3" -> s3Provider
            else -> throw IllegalArgumentException("unknown remote provider '$type'")
        }
    }
}

internal fun ApplicationCompressionConfiguration(): Compression.Configuration.() -> Unit {
    return {
        gzip {
            priority = 1.0
        }
        deflate {
            priority = 10.0
            minimumSize(1024) // condition
        }
    }
}

@KtorExperimentalAPI
fun Application.main() {
    val providers = ProviderModule(System.getenv("TITAN_POOL") ?: "titan")
    providers.operation.loadState()
    mainProvider(providers)
}

@KtorExperimentalAPI
fun Application.mainProvider(providers: ProviderModule) {
    install(DefaultHeaders)
    val gsonConverter = GsonConverter(ModelTypeAdapters.configure(GsonBuilder()).create())
    install(ContentNegotiation) {
        register(ContentType.Application.Json, gsonConverter)
        register(ContentType.parse("application/vnd.docker.plugins.v1.2+json"), gsonConverter)
        // Docker is really sloppy with setting Content-Type, so we need a default behavior
        register(ContentType.Any, gsonConverter)
    }
    install(CallLogging) {
        level = Level.INFO
    }
    install(Compression, ApplicationCompressionConfiguration())
    install(Routing) {
        CommitsApi(providers)
        VolumeApi(providers)
        OperationsApi(providers)
        RemotesApi(providers)
        RepositoriesApi(providers)
    }
    install(StatusPages) {
        exception<NoSuchObjectException> { cause ->
            call.respond(HttpStatusCode.NotFound, exceptionToError(call.request, cause))
            call.application.log.info(cause.message)
        }
        exception<ObjectExistsException> { cause ->
            call.respond(HttpStatusCode.Conflict, exceptionToError(call.request, cause))
            call.application.log.info(cause.message)
        }
        exception<IllegalArgumentException> { cause ->
            call.respond(HttpStatusCode.BadRequest, exceptionToError(call.request, cause))
            call.application.log.info(cause.message)
        }
        exception<JsonSyntaxException> { cause ->
            call.respond(HttpStatusCode.BadRequest, exceptionToError(call.request, cause))
            call.application.log.info(cause.message)
        }
        exception<Throwable> { cause ->
            call.respond(HttpStatusCode.InternalServerError, exceptionToError(call.request, cause))
            // For internal errors, log the whole exception and stack trace
            throw cause
        }
    }
}

@KtorExperimentalAPI
fun main(args: Array<String>) {
    val server = embeddedServer(CIO, (System.getenv("TITAN_PORT") ?: "5001").toInt(),
            module = Application::main)
    server.start(wait = true)
}
