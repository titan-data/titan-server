#!/bin/bash

#
# Get the desired size of the pool. Since the pool will be built on a sparse file, this is really
# more about determining the maximum size of the pool. We accomplish this by observing the amount
# of free space in the filesystem, and then carving off some space for various metadata. This will
# return the size as a string that can be passed to zpool create (e.g. "1043M"). If the df(1M)
# command itself fails for some reason, then it will exit with an error.
#
function get_pool_size() {
  local dir=$1
  local avail=$(df -BM --output=avail $dir | tail -1)
  [[ -z "$avail" ]] && log_error "Unable to determine available space for $dir"
  local size=$(( ${avail%%M} - 32 ))
  echo "${size}M"
  return 0
}

#
# This function will import the pool if it exists, otherwise it will create a new pool. If either
# the import or the creation fails, it will log an error and exit.
#
function create_import_pool() {
  local pool=$1
  local pool_dir=$2
  local mnt_dir=$3
  local data=$pool_dir/data
  local cachefile=$pool_dir/cachefile

  mkdir -p $pool_dir
  if ! pool_exists $pool; then
    if [ ! -f $cachefile ]; then
      log_start "Creating storage pool"
      local size=$(get_pool_size $pool_dir)
      truncate -s $size $data || log_error "Failed to create $size data file for storage pool"
      if ! create_pool $pool $data $mnt_dir $cachefile; then
        rm -f $data
        log_error "Failed to create storage pool"
      fi
    else
      log_start "Importing storage pool"
      if ! import_pool $cachefile $pool; then
        log_error "Failed to import existing storage pool"
      fi
    fi
    log_end
  fi
  return 0
}

#
# Sets up a shared bind mount beneath the pool mountpoint. This is somewhat convoluted, but serves
# an important purpose. The titan server will mount files within the /var/lib/<pool>/mnt, but by
# default these will only show up in the container namespace and not the host VM namespace. By
# creating a shared mount, these new mounts will propagate to the host VM. But that this point,
# that directory may or may not be a mountpoint. So we first create a bind mount to itself
# (if needed), thereby turning it into mountpoint. For more information, see:
#
# https://lwn.net/Articles/689856/
#
function bind_mounts() {
  local mnt_dir=$1

  log_start "Creating shared mounts"
  nsenter -m -u -t 1 -n -i sh -c \
    "set -xe
    if [ $(mount |grep $mnt_dir | wc -l) -eq 0 ]; then
        mkdir -p $mnt_dir && \
        mount --bind $mnt_dir $mnt_dir && \
        mount --make-shared $mnt_dir;
    fi" || log_error "Failed to create shared mounts"
  log_end
}

#
# Undoes the above operationa.
#
function unbind_mounts() {
  local mnt_dir=$1

  nsenter -m -u -t 1 -n -i sh -c \
    "set -xe
    if [ $(mount |grep $mnt_dir | wc -l) -ne 0 ]; then
        umount $mnt_dir;
    fi" || log_error "Failed to unbind shared mounts"
}

#
# Creates a bridge network specifically for titan. This is not generally required for normal operation, but provides
# better isolation and makes endtoend testing easier by providing an isolated network within which to run the
# SSH server.
#
function create_network() {
  local identity=$1
  docker network inspect $identity || docker network create $identity || log_error "failed to create $identity network"
}

#
# Remove the bridge network created for the titan server.
#
function remove_network() {
  local identity=$1
  docker network rm $identity
}

#
# Launch the actual titan server.
#
function launch_server() {
  local identity=$1
  local pool=$2
  local port=$3
  local image=$4
  local mount=$5

  # Remove any stale docker plugin socket and make sure the plugins directory exists
  mkdir -p /run/docker/plugins
  rm -f /run/docker/plugins/$identity.sock

  # Remove any previous version of the server, in case it's stopped
  docker rm -f $identity-server >/dev/null 2>&1

  # Run the actual server image. Runs in the foreground so it remains part of the launch container
  # and the launch container will restart if it restarts.
  docker run -i --privileged --name=$identity-server \
      -v /var/run/docker.sock:/var/run/docker.sock \
      -v /run/docker/plugins:/run/docker/plugins \
      -v $mount:$mount:rshared \
      -e TITAN_POOL=$pool \
      -p $port:5001 \
      --network $identity \
      $image \
      /titan/run
}
