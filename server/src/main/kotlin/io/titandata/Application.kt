/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata

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
import io.titandata.apis.CommitsApi
import io.titandata.apis.DockerVolumeApi
import io.titandata.apis.OperationsApi
import io.titandata.apis.RemotesApi
import io.titandata.apis.RepositoriesApi
import io.titandata.apis.VolumesApi
import io.titandata.context.RuntimeContext
import io.titandata.context.docker.DockerZfsContext
import io.titandata.exception.NoSuchObjectException
import io.titandata.exception.ObjectExistsException
import io.titandata.metadata.MetadataProvider
import io.titandata.models.Error
import io.titandata.models.docker.DockerVolumeResponse
import io.titandata.orchestrator.CommitOrchestrator
import io.titandata.orchestrator.OperationOrchestrator
import io.titandata.orchestrator.Reaper
import io.titandata.orchestrator.RemoteOrchestrator
import io.titandata.orchestrator.RepositoryOrchestrator
import io.titandata.orchestrator.VolumeOrchestrator
import io.titandata.remote.RemoteServer
import java.io.PrintWriter
import java.io.StringWriter
import java.util.ServiceLoader
import org.slf4j.event.Level

fun exceptionToError(request: ApplicationRequest, t: Throwable): Any {
    if (request.path().startsWith("/VolumeDriver.")) {
        return DockerVolumeResponse(err = t.message ?: "unknown error")
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

class ServiceLocator(pool: String, inMemory: Boolean = true) {
    private val loader = ServiceLoader.load(RemoteServer::class.java)
    private val remoteProviders: MutableMap<String, RemoteServer>

    init {
        val providers = mutableMapOf<String, RemoteServer>()
        loader.forEach {
            if (it != null) {
                providers[it.getProvider()] = it
            }
        }
        remoteProviders = providers
    }

    private val dockerZfsContext = DockerZfsContext(pool)

    val metadata = MetadataProvider(inMemory)
    val commits = CommitOrchestrator(this)
    val repositories = RepositoryOrchestrator(this)
    val operations = OperationOrchestrator(this)
    val volumes = VolumeOrchestrator(this)
    val remotes = RemoteOrchestrator(this)
    val reaper = Reaper(this)

    val gson = GsonBuilder().create()

    // Return the default runtime context
    val context: RuntimeContext
        get() = dockerZfsContext

    // Get a remote provider by name
    fun remoteProvider(type: String): RemoteServer {
        return remoteProviders[type]
                ?: throw IllegalArgumentException("unknown remote provider '$type'")
    }

    // For testing purposes
    fun setRemoteProvider(type: String, server: RemoteServer) {
        remoteProviders[type] = server
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
    val services = ServiceLocator(System.getenv("TITAN_POOL") ?: "titan", false)
    services.metadata.init()
    Thread(services.reaper).start()
    services.operations.loadState()
    mainProvider(services)
}

@KtorExperimentalAPI
fun Application.mainProvider(services: ServiceLocator) {
    install(DefaultHeaders)
    val gsonConverter = GsonConverter(GsonBuilder().create())
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
        CommitsApi(services)
        DockerVolumeApi(services)
        OperationsApi(services)
        RemotesApi(services)
        RepositoriesApi(services)
        VolumesApi(services)
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
