#!/bin/bash

#
# Minimum ZFS version. Starting in version 0.8.0, the community is going to attempt to maintain
# backwards compatability, such that older versions of the utilities will continue to run against
# newer versions of the kernel modules.
min_zfs_version=0.8

#
# Return the tag in the ZFS repository we should be using to build ZFS binaries.
#
function get_zfs_build_version() {
  echo "0.8.1"
}

#
# While exact semver-style semantics have not been declared, we will adopt a semver style
# comparison for determining compatibility:
#
#   * Major version must be equivalent
#   * Minor version must be greater than or equal
#   * Patch or additional qualifiers (e.g. "-rc0") are ignored.
#
# The minimum version should therefore only be expressed as a major/minor pair and not include
# the patch version. In the event that the community decides to go a different direction with
# versioning compatibility, we will have to revisit this check. This function also treats the
# empty string as an incompatible version, to simplify callers that may get an empty version
# when checking the running system or filesystem modules.
#
function zfs_version_compatible() {
  [ -z "$1" ] && return 1
  local min_components=(${min_zfs_version//./ })
  local req_version=${1%.*}
  local req_components=(${req_version//./ })
  [ ${min_components[0]} -ne ${req_components[0]} ] && return 1
  [ ${min_components[1]} -gt ${req_components[1]} ] && return 1
  return 0
}

#
# Get the URL for a given asset hosted in the titan community downloads
#
function get_asset_url() {
  local asset_name=$1
  echo "https://download.titan-data.io/zfs-releases/$asset_name"
}

#
# Determine if the ZFS module is currently loaded. To do this, we look at lsmod output, looking for
# the ZFS module. Technically, ZFS is comprised of multiple modules such as zfs and spl, but the
# 'zfs' depends on all the others, so if it is loaded then we should be good.
#
function is_zfs_loaded() {
  lsmod | grep "^zfs " >/dev/null 2>&1
}

#
# Get the current running ZFS module, or return the empty string if the ZFS module is not currently
# loaded. This information is stored in /sys/module/zfs/version, and is available from within
# a container.
#
function get_running_zfs_version() {
  cat /sys/module/zfs/version 2>/dev/null
}

#
# Get the the ZFS module version from modules on the filesystem. This uses modinfo(8) to fetch
# the information from a directory specified as an argument. We first run depmod(8) in case we
# haven't created the requisite links yet (e.g. this was just built from source)
#
function get_filesystem_zfs_version() {
  local directory=$1
  depmod -b $directory >/dev/null 2>&1
  modinfo -F version -b $directory zfs 2>/dev/null
}

#
# Load ZFS module from a specific directory. This will use depmod(8) to ensure we have our
# dependencies built, and then modprobe(8) to actually load the modules. This does not generate
# any output, but will return success if the commands succeed.
#
function load_zfs_module() {
  local directory=$1
  depmod -b $directory >/dev/null 2>&1
  modprobe -d $directory zfs >/dev/null 2>&1
}

#
# Get the download URL of the preocmpiled module, if one exists. Returns the empty string if
# there is no known asset for the current version.
#
function get_precompiled_module_url() {
  get_asset_url zfs-$(get_zfs_build_version)-$(uname -r).tar.gz
}

#
# Download and extract the precompiled version of ZFS to the given directory.
#
function extract_precompiled_module() {
  local asset_url=$1
  local dstdir=$2
  curl -fsSL $asset_url > $dstdir/zfs.tar.gz || return 1
  cd $dstdir && tar -xzf zfs.tar.gz || return 1
  rm $dstdir/zfs.tar.gz
  return 0
}

#
# Check for /dev/zfs and create it if it exists. Device links are created at the time the container
# launches, so if we install the ZFS kernel module within the kernel we have to come back and
# create it manually. It will be present the next time the container starts.
#
function check_zfs_device() {
  if [[ ! -e /dev/zfs ]]; then
      mknod -m 660 /dev/zfs c $(cat /sys/class/misc/zfs/dev |sed 's/:/ /g') >/dev/null 2>&1
  fi
}

#
# Sanity check that ZFS is working properly. This just runs a few commands that should succeed,
# and is not exhaustive by any means.
#
function sanity_check_zfs() {
  zpool list >/dev/null 2>&1 || return 1
  zfs list >/dev/null 2>&1 || return 1
  return 0
}

#
# Return true if the given pool exists.
#
function pool_exists() {
  local pool=$1
  zpool status $pool >/dev/null 2>&1
}

#
# Import the named pool from the given cachefile.
#
function import_pool() {
  local cachefile=$1
  local pool=$2
  zpool import -f -c $cachefile $pool >/dev/null
}

#
# Create a new pool. We create the pool with an alternate cachefile so that it's never imported
# automatically when the docker host restarts - only when the launch container is started. This
# ensures that we can store data on docker volumes that might not be available when the system
# boots.
#
function create_pool() {
  local pool=$1
  local data=$2
  local mountpoint=$3
  local cachefile=$4
  zpool create -m $mountpoint -o cachefile=$cachefile $pool $data
  zfs create -o mountpoint=none -o compression=lz4 $pool/repo
  zfs create -o mountpoint=none $pool/deathrow
}

#
# Destroy a pool.
#
function destroy_pool() {
  local pool=$1
  zpool destroy $pool
}

#
# Check to see if ZFS is loaded and, if so, whether it's compatible. This will return 0 if the
# running ZFS is compatible, 1 if it's not loaded, or exit on failure if it's loaded but
# incompatible.
#
# If we update to a new ZFS version but don't restart the docker host, we may find that we have
# an older ZFS version already loaded. We can't simply unload & re-install, as we may have active
# containers in use. If and when we come to that point, we'll need to coordinate with the CLI
# to stop all repositories, unload ZFS through titan, and re-install.
#
function check_running_zfs() {
  log_start "Checking if compatible ZFS is running"
  local retval=1
  if is_zfs_loaded; then
    local version=$(get_running_zfs_version)
    if ! zfs_version_compatible $version; then
      log_error "System is running ZFS $version incompatible with $min_zfs_version, upgrade and retry"
    fi
    echo "System is running ZFS version $version"
    retval=0
  else
    echo "ZFS is not currently loaded"
  fi
  log_end
  return $retval
}

#
# Check to see if a compatible ZFS version exists on the system, loading it if it's available.
# Returns success if it's found, compatible, and can be loaded; failure otherwise. This takes
# both a directory location, but then also a type descriptor for messages, as we'll use the
# same method to attempt to load the previously built ZFS modules.
#
function load_zfs() {
  local module_dir=$1
  local module_type=$2
  local install_dir=$3
  local retval=1

  log_start "Checking if compatible $module_type ZFS is available"
  version=$(get_filesystem_zfs_version $module_dir)
  if zfs_version_compatible $version; then
    echo "Version $version compatible"
    if load_zfs_module $module_dir; then
      echo "ZFS loaded"
      echo $module_dir > $install_dir/installed_zfs
      retval=0
    else
      echo "Failed to load module"
    fi
  else
    if [[ -z "$version" ]]; then
      echo "No ZFS module found"
    else
      echo "Version $version incompatible with $min_zfs_version"
    fi
  fi
  log_end
  return $retval
}

#
# Check to see if precompiled source for the current kernel exists, and load it if found. Returns
# success if loaded, failure if it's not found or fails to load.
#
function load_precompiled_zfs() {
  local dstdir=$1
  local install_dir=$2
  local uname=$(uname -r)
  local retval=1

  log_start "Checking if precompiled ZFS is available for '$uname'"
  # Remove any previous contents and recreate just in case there was some kind of old or
  # incompatible module
  rm -rf $dstdir || return 1
  mkdir -p $dstdir || return 1
  local asset_url=$(get_precompiled_module_url)
  if extract_precompiled_module $asset_url $dstdir; then
    echo "Version $uname extracted to $dstdir"
    if load_zfs_module $dstdir; then
      echo "Version $uname loaded"
      echo $dstdir > $install_dir/installed_zfs
      retval=0
    else
      echo "Failed to load precompiled module"
    fi
  else
    echo "No ZFS module found"
  fi
  log_end
  return $retval
}

#
# Launch the zfs-builder image to compile ZFS modules for the current kernel.
#
function compile_and_load_zfs() {
  local dstdir=$1
  local install_dir=$2

  log_start "Building ZFS kernel modules (this could take 30 minutes, submit a request for $(uname -r) prebuilt binaries)"
  mkdir -p $dstdir
  docker run --rm -v $dstdir:/build \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -e ZFS_VERSION=zfs-$(get_zfs_build_version) \
    -e ZFS_CONFIG=kernel titandata/zfs-builder:latest || log_error "ZFS build failed"
  log_end

  if ! load_zfs_module $dstdir; then
    log_error "Failed to load compiled modules"
  fi

  echo $dstdir > $install_dir/installed_zfs
}

#
# Check that ZFS is functioning, creating the ZFS device if needed and running a few sanity tests.
# If the commands fail, then we log an error and exit.
#
function check_zfs() {
  check_zfs_device
  sanity_check_zfs || log_error "ZFS not configured properly, contact help"
}

#
# Unloads the ZFS module if and only if it's currently loaded and we loaded it in the first place.
#
function unload_zfs() {
  local install_dir=$1
  if [[ is_zfs_loaded && -f $install_dir/installed_zfs ]]; then
    local module_location=$(cat $install_dir/installed_zfs)
    modprobe -d "$module_location" -r zfs || return 1
  fi
  return 0
}
