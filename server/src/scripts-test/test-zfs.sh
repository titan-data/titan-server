#!/usr/bin/env bats

zfs_script=/test/src/scripts/zfs.sh
util_script=/test/src/scripts/util.sh

@test "zfs loaded check succeeds with valid lsmod output" {
  source $zfs_script
  function lsmod() { echo "zfs "; }
  export -f lsmod
  run is_zfs_loaded
}

@test "zfs loaded check fails with no zfs module" {
  source $zfs_script
  function lsmod() { echo "nothing"; }
  export -f lsmod
  run is_zfs_loaded
  [ $status -eq 1 ]
}

@test "zfs loaded check fails with invalid zfs module" {
  source $zfs_script
  function lsmod() { echo " zfsy"; }
  export -f lsmod
  run is_zfs_loaded
  [ $status -eq 1 ]
}

@test "get running zfs version returns output of ZFS module" {
  source $zfs_script
  function cat() { echo "0.8.0"; }
  export -f cat
  run get_running_zfs_version
  [ $status -eq 0 ]
  [ "$output" = "0.8.0" ]
}

@test "get running zfs version returns empty string if not loaded" {
  source $zfs_script
  function cat() { /bin/false; }
  export -f cat
  run get_running_zfs_version
  [ $status -eq 1 ]
  [ -z "$output" ]
}

@test "get filesystem zfs version returns output from modinfo" {
  source $zfs_script
  function depmod() { /bin/true; }
  function modinfo() { echo "0.8.0"; }
  export -f depmod modinfo
  run get_filesystem_zfs_version /system/lib
  [ $status -eq 0 ]
  [ "$output" = "0.8.0" ]
}

@test "get filesystem zfs version returns empty string if modinfo fails" {
  source $zfs_script
  function depmod() { /bin/true; }
  function modinfo() { /bin/false; }
  export -f depmod modinfo
  run get_filesystem_zfs_version /system/lib
  [ $status -eq 1 ]
  [ -z "$output" ]
}

@test "load zfs module succeeds if modprobe succeeds" {
  source $zfs_script
  function depmod() { /bin/true; }
  function modprobe() { /bin/true; }
  export -f depmod modprobe
  run load_zfs_module /system/lib
  [ $status -eq 0 ]
  [ -z "$output" ]
}

@test "load zfs module fails if modprobe fails" {
  source $zfs_script
  function depmod() { /bin/true; }
  function modprobe() { /bin/false; }
  export -f depmod modprobe
  run load_zfs_module /system/lib
  [ $status -eq 1 ]
  [ -z "$output" ]
}

@test "load zfs module succeeds if depmod fails but modprobe succeeds" {
  source $zfs_script
  function depmod() { /bin/false; }
  function modprobe() { /bin/true; }
  export -f depmod modprobe
  run load_zfs_module /system/lib
  [ $status -eq 0 ]
  [ -z "$output" ]
}

@test "exact minimum zfs version is compatible" {
  source $zfs_script
  run zfs_version_compatible "0.8"
  [ $status -eq 0 ]
}

@test "patch zfs version is ignored" {
  source $zfs_script
  run zfs_version_compatible "0.8.3"
  [ $status -eq 0 ]
}

@test "zfs version qualifier is ignored" {
  source $zfs_script
  run zfs_version_compatible "0.8.3-rc5"
  [ $status -eq 0 ]
}

@test "greater minor version is compatible" {
  source $zfs_script
  run zfs_version_compatible "0.9.3-rc5"
  [ $status -eq 0 ]
}

@test "lesser minor version is incompatible" {
  source $zfs_script
  run zfs_version_compatible "0.7.1"
  [ $status -eq 1 ]
}

@test "different major version is incompatible" {
  source $zfs_script
  run zfs_version_compatible "1.8.0"
  [ $status -eq 1 ]
}

@test "empty version is incompatible" {
  source $zfs_script
  run zfs_version_compatible ""
  [ $status -eq 1 ]
}

@test "zfs compatibility check fails if ZFS not running" {
  function docker() { /bin/true; }
  function jq() { /bin/true; }

  source $zfs_script
  source $util_script
  function lsmod() { echo "nothing"; }
  export -f lsmod
  run check_running_zfs
  [ $status -eq 1 ]
  [[ "$output" == *"not currently loaded"* ]]
}

@test "zfs compatibility check fails if ZFS installed but invalid version" {
  function docker() { /bin/true; }
  function jq() { /bin/true; }

  source $zfs_script
  source $util_script
  function lsmod() { echo "zfs "; }
  function cat() { echo "0.7.0"; }
  export -f lsmod cat
  run check_running_zfs
  [ $status -eq 1 ]
  [[ "$output" == *"incompatible"* ]]
}

@test "zfs compatbility check succeeds if ZFS installed and valid version" {
  function docker() { /bin/true; }
  function jq() { /bin/true; }

  source $zfs_script
  source $util_script
  function lsmod() { echo "zfs "; }
  function cat() { echo "0.8.0"; }
  export -f lsmod cat
  run check_running_zfs
  [ $status -eq 0 ]
}

@test "load zfs fails if no module found" {
  function docker() { /bin/true; }
  function jq() { /bin/true; }

  source $zfs_script
  source $util_script
  function depmod() { /bin/true; }
  function modinfo() { /bin/false; }
  export -f depmod modinfo
  run load_zfs /system/lib system
  [ $status -eq 1 ]
  [[ "$output" == *"No ZFS module found"* ]]
}

@test "load zfs fails if version is incompatible" {
  function docker() { /bin/true; }
  function jq() { /bin/true; }

  source $zfs_script
  source $util_script
  function depmod() { /bin/true; }
  function modinfo() { echo "0.7.0"; }
  export -f depmod modinfo
  run load_zfs /system/lib system
  [ $status -eq 1 ]
  [[ "$output" == *"incompatible"* ]]
}

@test "load zfs fails if module fails to load" {
  function docker() { /bin/true; }
  function jq() { /bin/true; }

  source $zfs_script
  source $util_script
  function depmod() { /bin/true; }
  function modinfo() { echo "0.8.0"; }
  function modprobe() { /bin/false; }
  export -f depmod modinfo modprobe
  run load_zfs /system/lib system
  [ $status -eq 1 ]
  [[ "$output" == *"Failed to load"* ]]
}

@test "load zfs succeeds if modules can be loaded" {
  function docker() { /bin/true; }
  function jq() { /bin/true; }

  source $zfs_script
  source $util_script
  local testdir=/tmp/test.$$
  function depmod() { /bin/true; }
  function modinfo() { echo "0.8.0"; }
  function modprobe() { /bin/true; }
  export -f depmod modinfo modprobe
  mkdir -p $testdir
  run load_zfs /system/lib system $testdir
  [ $status -eq 0 ]
  [ -f $testdir/installed_zfs ]
  rm -rf $testdir
}

@test "extract precompiled modules fail if curl fails" {
  function docker() { /bin/true; }
  function jq() { /bin/true; }
  function curl() { /bin/false; }
  function uname() { echo "foo"; }

  export -f curl uname

  source $zfs_script
  source $util_script
  local testdir=/tmp/test.$$
  mkdir -p $testdir
  run extract_precompiled_module /no/such/directory
  rm -rf $testdir
  [ $status -eq 1 ]
}

@test "extract precompiled source fails if tar fails" {
  function docker() { /bin/true; }
  function jq() { /bin/true; }
  function curl() { /bin/true; }
  function uname() { echo "foo"; }
  function tar() { /bin/false; }

  export -f curl uname

  source $zfs_script
  source $util_script
  local testdir=/tmp/test.$$
  mkdir -p $testdir
  run extract_precompiled_module /no/such/directory
  rm -rf $testdir
  [ $status -eq 1 ]
}

@test "extract precompiled source succeeds" {
  function docker() { /bin/true; }
  function jq() { /bin/true; }
  function curl() { /bin/true; }
  function uname() { echo "foo"; }
  function tar() { /bin/true; }

  export -f curl uname

  source $zfs_script
  source $util_script
  local testdir=/tmp/test.$$
  mkdir -p $testdir
  run extract_precompiled_module /no/such/directory
  rm -rf $testdir
  [ $status -eq 0 ]
}

@test "load precompiled module fails if asset does not exist" {
  function docker() { /bin/true; }
  function jq() { /bin/true; }

  source $zfs_script
  source $util_script
  local testdir=/tmp/test.$$
  function uname() { echo "foo"; }
  function curl() { /bin/false; }
  export -f uname curl

  run load_precompiled_zfs $testdir $testdir
  [ $status -eq 1 ]
  [[ "$output" == *"No ZFS module found"* ]]
}

@test "load precompiled module fails if tar fails" {
  function docker() { /bin/true; }
  function jq() { echo "foo"; }

  source $zfs_script
  source $util_script
  local testdir=/tmp/test.$$
  function uname() { echo "foo"; }
  function tar() { /bin/false; }
  function curl() { /bin/true; }
  export -f uname tar curl

  mkdir -p $testdir/dst
  touch $testdir/foo.tar.gz
  run load_precompiled_zfs $testdir $testdir/dst
  [ $status -eq 1 ]
  [[ "$output" == *"No ZFS module found"* ]]
  rm -rf $testdir
}

@test "load precompiled module fails if modprobe fails" {
  function docker() { /bin/true; }
  function jq() { echo "foo"; }

  source $zfs_script
  source $util_script
  local testdir=/tmp/test.$$
  function uname() { echo "foo"; }
  function tar() { /bin/true; }
  function depmod() { /bin/true; }
  function modprobe() { /bin/false; }
  function curl() { /bin/true; }
  export -f uname tar depmod modprobe curl

  mkdir -p $testdir/dst
  touch $testdir/foo.tar.gz
  run load_precompiled_zfs $testdir $testdir/dst
  [ $status -eq 1 ]
  [[ "$output" == *"Failed to load"* ]]
  rm -rf $testdir
}

@test "load precompiled module succeeds" {
  function docker() { /bin/true; }
  function jq() { echo "foo"; }

  source $zfs_script
  source $util_script
  local testdir=/tmp/test.$$
  function uname() { echo "foo"; }
  function tar() { /bin/true; }
  function depmod() { /bin/true; }
  function modprobe() { /bin/true; }
  function curl() { /bin/true; }
  export -f uname tar depmod modprobe curl

  mkdir -p $testdir/dst
  touch $testdir/foo.tar.gz
  run load_precompiled_zfs $testdir/dst $testdir
  [ $status -eq 0 ]
  [ -f $testdir/installed_zfs ]
  rm -rf $testdir
}

@test "check zfs device fails if mknod fails" {
  source $zfs_script
  function mknod() { /bin/false; }
  export -f mknod

  rm -f /dev/zfs
  run check_zfs_device
  [ $status -eq 1 ]
}

@test "check zfs device succeeds if /dev/zfs exists" {
  source $zfs_script
  function mknod() { /bin/false; }
  export -f mknod

  touch /dev/zfs
  run check_zfs_device
  [ $status -eq 0 ]
}

@test "check zfs device invokes mknod if /dev/zfs not present" {
  source $zfs_script
  function mknod() { /bin/true; }
  export -f mknod

  rm -f /dev/zfs
  run check_zfs_device
  [ $status -eq 0 ]
}

@test "zfs sanity test succeeds" {
  source $zfs_script
  function zfs() { /bin/true; }
  function zpool() { /bin/true; }
  export -f zfs zpool

  run sanity_check_zfs
  [ $status -eq 0 ]
}

@test "zfs sanity test fails if zfs fails" {
  source $zfs_script
  function zfs() { /bin/false; }
  function zpool() { /bin/true; }
  export -f zfs zpool

  run sanity_check_zfs
  [ $status -eq 1 ]
}

@test "zfs sanity test fails if zpool fails" {
  source $zfs_script
  function zfs() { /bin/true; }
  function zpool() { /bin/false; }
  export -f zfs zpool

  run sanity_check_zfs
  [ $status -eq 1 ]
}

@test "check zfs succeeds" {
  source $zfs_script

  function zfs() { /bin/true; }
  function zpool() { /bin/true; }
  function mknod() { /bin/true; }
  export -f mknod zfs zpool

  run check_zfs
  [ $status -eq 0 ]
}

@test "check zfs fails if zpool fails" {
  function docker() { /bin/true; }
  function jq() { /bin/true; }

  source $zfs_script
  source $util_script

  function zfs() { /bin/true; }
  function zpool() { /bin/false; }
  function mknod() { /bin/true; }
  export -f mknod zfs zpool

  run check_zfs
  [ $status -eq 1 ]
}

@test "pool exists fails if zpool fails" {
  source $zfs_script
  function zpool() { /bin/false; }
  export -f zpool

  run pool_exists foo
  [ $status -eq 1 ]
}

@test "pool exists succeeds if zpool succeeds" {
  source $zfs_script
  function zpool() { /bin/true; }
  export -f zpool

  run pool_exists foo
  [ $status -eq 0 ]
}

@test "pool import fails if zpool fails" {
  source $zfs_script
  function zpool() { /bin/false; }
  export -f zpool

  run import_pool /cachefile foo
  [ $status -eq 1 ]
}

@test "pool import succeeds if zpool succeeds" {
  source $zfs_script
  function zpool() { /bin/true; }
  export -f zpool

  run import_pool /cachefile foo
  [ $status -eq 0 ]
}

@test "pool destroy fails if zpool fails" {
  source $zfs_script
  function zpool() { /bin/false; }
  export -f zpool

  run destroy_pool foo
  [ $status -eq 1 ]
}

@test "pool destroy succeeds if zpool succeeds" {
  source $zfs_script
  function zpool() { /bin/true; }
  export -f zpool

  run destroy_pool foo
  [ $status -eq 0 ]
}

@test "unload zfs succeeds if modules is not loaded" {
  source $zfs_script
  function lsmod() { /bin/true; }
  function modprobe() { /bin/false; }
  export -f lsmod modprobe

  run unload_zfs /dir
  [ $status -eq 0 ]
}

@test "unload zfs succeeds if modules were not installed by titan" {
  source $zfs_script

  function lsmod() { /bin/true; }
  function modprobe() { /bin/false; }
  export -f lsmod modprobe

  local testdir=/tmp/test.$$
  run unload_zfs $testdir
  [ $status -eq 0 ]
}

@test "unload zfs succeeds if modprobe succeeds" {
  source $zfs_script

  function lsmod() { echo "zfs "; }
  function modprobe() { /bin/true; }
  export -f lsmod modprobe

  local testdir=/tmp/test.$$
  mkdir -p $testdir
  touch $testdir/installed_zfs

  run unload_zfs $testdir
  [ $status -eq 0 ]
  rm -rf $testdir
}

@test "unload zfs fails if modprobe fails" {
  source $zfs_script

  function lsmod() { echo "zfs "; }
  function modprobe() { /bin/false; }
  export -f lsmod modprobe

  local testdir=/tmp/test.$$
  mkdir -p $testdir
  touch $testdir/installed_zfs

  run unload_zfs $testdir
  [ $status -eq 1 ]
  rm -rf $testdir
}

@test "compile zfs succeeds" {
  function docker() { /bin/true; }
  function jq() { /bin/true; }

  source $zfs_script
  source $util_script

  function depmod() { /bin/true; }
  function modprobe() { /bin/true; }
  function docker() { /bin/true; }
  export -f depmod modprobe docker

  local testdir=/tmp/test.$$
  mkdir -p $testdir
  run compile_and_load_zfs $testdir/dst $testdir
  [ $status -eq 0 ]
  [ -f $testdir/installed_zfs ]
  rm -rf $testdir
}

@test "compile zfs fails if docker fails" {
  function docker() { /bin/true; }
  function jq() { /bin/true; }

  source $zfs_script
  source $util_script

  function depmod() { /bin/true; }
  function modprobe() { /bin/true; }
  function docker() { /bin/false; }
  export -f depmod modprobe docker

  local testdir=/tmp/test.$$
  mkdir -p $testdir
  run compile_and_load_zfs $testdir/dst $testdir
  [ $status -eq 1 ]
  [ ! -f $testdir/installed_zfs ]
  rm -rf $testdir
}

@test "compile zfs fails if modprobe fails" {
  function docker() { /bin/true; }
  function jq() { /bin/true; }

  source $zfs_script
  source $util_script

  function depmod() { /bin/true; }
  function modprobe() { /bin/false; }
  function docker() { /bin/true; }
  export -f depmod modprobe docker

  local testdir=/tmp/test.$$
  mkdir -p $testdir
  run compile_and_load_zfs $testdir/dst $testdir
  [ $status -eq 1 ]
  [ ! -f $testdir/installed_zfs ]
  rm -rf $testdir
}
