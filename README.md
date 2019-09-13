# Titan Docker Server

This repository contains the docker container that is used by the titan CLI. This docker container
provides all the repository operations necessary to support titan-powered containers, along with 
remote providers to push & and pull data between repositories.

For more information about titan, see the titan CLI repository. The remainder of this README will
explain the underlying architecture for titan developers or those interested in the implementation
details.

## Overall architecture

There are a few key components of the overall architecture:

  * `titan-server` - A kotlin web server that provides an API for the titan CLI as well as the 
     docker volume API.
  * `titan-client` - A JAR providing a HTTP client for use with `titan-server`, used by the CLI to
     make API requests to the server.
  * `titan` - A Docker image that provides a runtime environment for `titan-server`, as well as
     the means to execute ZFS commands within that container.
      
Of these, the docker image is by far the most complicated, as we have to go through several
different hoops to get ZFS usable within containers on arbitrary host systems.

## Titan server and client

The titan server is built from `server/src`. It is a web server wrapped around a storage
persistence layer, and remote executor framework. The important parts of the server can be
found at:

  * `io.titandata.apis.*` - Entry points for the remote APIs. Very thin layer that translates
    from JSON into native models and then invokes the appropriate backend model.
  * `io.titandata.storage.*` - Provider for storage and metadata persistence. While there is
    an abstraction layer, there is only one provider for ZFS. See `ZfsStorageProvider` for more
    information.
  * `io.titandata.operation.*` - Handlers for asynchronous push and pull operations. The actual
    operations are run through the remote providers, but the generic operations framework handles
    starting and stopping operations, reporting back to the APIs, etc. For more information,
    see `OperationProvider`
  * `io.titandata.remote.*` - Per-remote handlers for push and pull operations. These handle the
    work of pushing and pulling, within the context set up by the operations provider.
  

The titan client is built from `client/src`, and creates a separate JAR that is then published
to an artifact repository for the CLI to use. It is not much more than a framework to marshall
data between the JAVA and JSON representation, though it does contain a few additional helper
routines, such as converting from a URI string to a remote object. The server references this
client as well so that it is sure to use the same data models.
 
The original client and server skeletons were generated via the openapi code generator. However,
due to limitations with how that generator deals with things like polymorphism, errors, and 
non-standard member names, we instead hand-craft our clients on top of this base. We do our
best to keep the official definition up to date in `server/src/main/resource/openapi.yaml`,
even though it's no longer used to generate files.
hand-crafted versions of the files.

## Container architecture

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
     across Docker upgrades that instantiate a new underlying VM.
  3. Configure the system such that mounts created within the `titan-server` container will
     correctly propagate to the host VM and then to other containers.
    
The first of these is covered extensively in the the comments to the
[launch script](server/src/scripts/launch), where we try to either leverage existing ZFS modules,
install pre-built ones, or build new ones on the fly. The matching ZFS userland comes pre-installed
on the docker image, so that the commands can be accessed in their normal locations within the
container.

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

## ZFS architecture

All of the ZFS filesystems are stored within the `titan` pool that is created by the launch container.
We therefore start with a single filesystem that is the root of our repository:

    titan/<repo>

The next thing we have to deal with is that we want to swap out the data state for this logical volume
without having to change the name, since it remains attached to the container. For this reason, we
assign every active filesystem state, called an instance, its own GUID:

    titan/<repo>/<guid>

We then set a ZFS user property on the repository (`io.titan-data:active`) that indicates the
currently active GUID. Any attempt to get info, mount, unmount, etc this logical volume will first
lookup that active path and redirect to that filesystem.

But docker containers can also contain multiple volumes. Ideally, we'd want to just treat these as
simple subdirectories, but docker doesn't allow for mounting of filesystems within volumes. And while
we could create some kind of namespace that maps many paths back to one filesystem, this creates
a level of complexity that isn't warranted given that ZFS can manage trees of filesystems easily.
The resulting tree looks like:

    titan/<repo>/<guid>/<vol>

The client is responsible for running the container on top of the given volumes. When we want to commit
a hash, we simply take a recursive snapshot of the currently active GUID. When we want to switch to
a previous state, we create a new GUID and clone each of the volumes into that space, updating the
active pointer to the new GUID. Because we've stopped the container in between these operations, we
can update its mountpoint as needed. Note that from a hygeine perspective, it would be better to 

Listing commits is therefor just listing all the snapshots for each of the GUIDs and assembling them
together into a single list. The commit message is stored as a ZFS user property on the snapshot
(`io.titan-data:message`).

## Build

There are two separate projects, a server project and a client project. The former is the bulk of
the implementation, the latter is a wrapper around the APIs to make it easy to work with the
server.

```
./gradlew build
```

This will build the server and client, run style checks, as well as unit tests and integration tests. It will
then package the server into the titan docker image. There is a third test target,
`endtoendTest`, that is not run automatically as part of `check` given how much longer it
can take. This should be run separately to perform the full barrage of tests, but will require
running the container, potentially connecting to external resources, etc. If you want to run the
end-to-end engine tests, you will need to specify the information required to connect to a
titan-enabled engine:

```
./gradlew endtoendTest -Pengine.connection=user:password@host
```

Similarly, running endtoend tests for S3 requires specifying the S3 location (combination of
bucket and key) as a property. You must have your AWS credentials configured via `.aws/config`
or environment variables, including access key, secret key, and region.

```
./gradlew endtoendTest -P s3.location=bucket/path
```

To publish the image to dockerhub and the client to artifactory, run:

```
./gradlew publish
```

This should really only be done by CI/CD automation, and will require you to `docker login` with
appropriate privileges prior to pushing to the `titandata/titan` image.

## ZFS Packaging

One of the challenges we face is that it is unlikely that ZFS will be installed within the docker
VM of a developer laptop, which means that we need to take ownership of installing it for them.
But being a kernel module, we need to build it against the currently running kernel. We provide
pre-built kernel binaries for common MacOS and Windows docker VM kernels, but can also build
the modules at install time if we don't recognize the kernel. We'll also use an installed ZFS
version if it's compatible with our userland utilities, resulting in the following options:

 * Use the running ZFS version if it's installed and an acceptable version. Fail if it's an
   incompatible version.
 * If we have pre-built kernel binaries for the given environment (from `uname -r`), then 
   install those.
 * Otherwise, dynamically build new kernel binaries and install those.
 
We use the `titandata/zfs-builder` docker image to do the builds themselves. This tool takes the
kernel version, zfs version, and kernel `config.gz` file to build for any kernel. To manage our
pre-built userland binaries and kernel packages, we track configurations in the following places
in the source trees:

  * `zfs/config/$(uname -r)/config.gz` - The `config.gz` file for the given kernel, as extracted
    from `/proc/config.gz` on the system. Running `./gradlew getZfsConfig` will get the
    configuration from the currently running docker VM and place it in the appropriate place.
  * `zfs/kernel/$(uname -r).tar.gz` - The kernel modules for the given kernel. These can be
    created by running `./gradlew buildZfsKernel [-Puname=...]`. Given a specific uname, it
    will build only that kernel. Otherwise, it will build the default kernel. There is not
    currently a task that will build all known ZFS configurations - it has to be done one by one.
  * `zfs/userland/*` - This contains the extracted userland binaries for ZFS, ready to be copied
    into the docker container. Since they should (as of 0.8.0) maintain compatibility with a wide
    range of past kernel modules, we don't need to rebuild them for every distro, but can simply
    use the latet ones. The `./gradlew buildZfsUserland` task will rebuild and package these
    files.
    
The ZFS version we use is stored in `gradle/zfs.gradle.kts`. In the event we want to bump the
ZFS version, you will need to rebuild the userland binaries and all kernel distros. In the event
that storing these binaries in-tree becomes too expensive, we can move them to a shared package
repository and pull just the one we need at install time. But for now this is sufficient.

## OpenAPI and Generated API files

