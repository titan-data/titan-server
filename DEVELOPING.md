# Project Development

For general information about contributing changes, see the
[Contributor Guidelines](https://github.com/titan-data/.github/blob/master/CONTRIBUTING.md).

## How it Works

There are a few key components of the overall architecture:

  * `titan-server` - A kotlin web server that provides an API for the titan CLI as well as the 
     docker volume API.
  * `titan-client` - A JAR providing a HTTP client for use with `titan-server`, used by the CLI to
     make API requests to the server, and manage remotes.
  * `titan` - A Docker image that provides a runtime environment for `titan-server`, as well as
     the means to execute ZFS commands within that container.
      
Of these, the docker image is by far the most complicated, as we have to go through several
different hoops to get ZFS usable within containers on arbitrary host systems.

Note that the project is in the process of being migrated from Kotlin to golang. During this
process, you will see some less than elegant worts, such as some tests in golang but others
in Kotlin, plugins in golang being loaded from Kotlin, etc.

### Titan server and client

The titan server is built from `server/src`. It is a web server wrapped around a storage
persistence layer, and remote executor framework. The important parts of the server can be
found at:

  * `io.titandata.apis.*` - Entry points for the remote APIs. Very thin layer that translates
    from JSON into native models and then invokes the appropriate backend model.
  * `io.titandata.metadata.*` - Metadata persistence layer. We run a PostgreSQL database within
    the server container.
  * `io.titandata.context.*` - Provider for context-specific operations, such as managing storage
    and running operations that requires access to storage.
  * `io.titandata.operation.*` - Handlers for asynchronous push and pull operations. The actual
    operations are run through the remote providers, but the generic operations framework handles
    starting and stopping operations, reporting back to the APIs, etc. For more information,
    see `OperationProvider`
  
The titan client is built from `client/src`, and creates a separate JAR that is then published
to an artifact repository for the CLI to use. It is not much more than a framework to marshall
data between the JAVA and JSON representation, though it does contain a few additional helper
routines, such as converting from a URI string to a remote object. The server references this
client as well so that it is sure to use the same data models.

### Container architecture

The underlying architecture can be a bit complex due to the intricacies of docker and ZFS. Our goal is
to be able to spin up a container that acts as docker volume plugin and has ZFS access to the docker host
VM (typically running inside of Windows or MacOS). Actually instructing users on how to install and
manage ZFS within this hidden VM is a non-starter, so we use a shell (`titan-launch`) container that
is responsible for setting up and configuring ZFS, only to then launch the server itself
(`titan-server`), such that the server can access the pool and ZFS commands without having to worry
about how that is made possible.

To do this, we have to accomplish a few key things:

  1. Install and configure ZFS to be able to run on the underlying VM kernel.
  2. Create a storage pool that we can use for storing data and metadata, one that will survive
     across Docker upgrades that instantiate a new underlying VM. For more information on how repositories are
     organized and metadata is tracked, see the `Zfs*Provider` classes, such as `ZfsOperationProvider.kt`.
  3. Configure the system such that mounts created within the `titan-server` container will
     correctly propagate to the host VM and then to other containers.
    
The first of these is covered extensively in the the comments to the
[launch script](server/src/scripts/launch), where we try to either leverage existing ZFS modules,
install pre-built ones, or build new ones on the fly. We first try to pull precompiled kernel binaries from
[zfs-releases](https://github.com/titan-data/zfs-releases), and fall back to using
[zfs-builder](https://github.com/titan-data/zfs-builder) to build custom modules if no
precompiled binaries can be found.

The second piece requires that we leverage an external volume (`titan-data`) to store any ZFS
storage pool or other data that we need to persist beyond the lifetime of the Docker VM. This
volume is created by the CLI and mounted at `/var/lib/titan/data`. We then create the following
directories:

   * `/var/lib/titan/data/pool` - The underlying file backing the storage pool we create
   * `/var/lib/titan/mnt` - Root mountpoint for all filesystems created in the pool. This must
     exist on the root VM and not within the `titan-data` volume in order for bind mounts to 
     work properly.
   * `/var/lib/titan/data/modules` - Stash of ZFS modules (built or installed) to match the current
     kernel.

The third piece is a nuance of how Linux filesystems work in container namespaces. For more
information, see the `bind_mounts()` function in the [titan.sh](server/src/scripts/titan.sh)
utility file.

To keep the packaging simple, we leverage the same image for both the launcher and the server,
just executed with different entry points ([launch](server/src/scripts/launch)
and [run](server/src/scripts/run)) to invoke the different capabilities. We also provide a third
entry point, [teardown](server/src/scripts/teardown), that will destroy the ZFS pool and unload
ZFS modules if they were installed by the launcher.


## Building

There are two separate projects, a server project and a client project. The former is the bulk of
the implementation, the latter is a wrapper around the APIs to make it easy to work with the
server.

```
./gradlew build
```

This will build the server and client, run style checks, as well as unit tests and integration tests. It will
then package the server into the titan docker image. If you run into lint or style errors, you can run
`./gradlew ktlintFormat` to automatically format the code.


## Testing

There are three types of tests:

  * Unit tests - These tests are designed to test a fragment of code, and can be run entirely within the IDE. These
    tests live under the `src/test` directory and are run through `./gradlew test`. They should be very fast and
    are run as part of each pull request.
  * Integration tests - These tests validate the whole titan server program, but still runs within the IDE and
    hence mock out operating system dependencies (such as ZFS). These tests live under `src/integrationTest` and
    can be run as part of `./gradlew integrationTest`. They should be fast and are run as part of each pull
    request.
  * End to end tests - These tests run against the complete docker container, and hence are able to test the full
    stack, including ZFS. These tests live under `test/endtoend` and are written in golang. These tests may be slow,
    may depend on external resources like S3 buckets), but should remain runnable through CI/CD automation during the
    release process. End to end tests are not run automatically as part of `check` given how much longer it
    can take. To run the tests, you will need to run:

    ```
    go test ./test/endtoend
    ```

    Some tests require additional configuration. In this case, you will need to specify the configuration as
    part of the environment, such as:

    ```
    S3_LOCATION=s3://my-bucket/test-data go test ./test/endtoend
    ```
    
These tests do generate coverage reports, but test coverage is not yet rigorously integrated into the development
process.

## Releasing

Releases are triggered via Travis when a new tag is pushed to the `master` branch. This will invoke `./gradlew publish`, 
which will publish the docker image to dockerhub, as well as the client JAR to the community maven S3 bucket for
use by the CLI.

