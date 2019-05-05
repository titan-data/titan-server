#!/usr/bin/env bats

titan_script=/test/src/scripts/titan.sh
util_script=/test/src/scripts/util.sh
zfs_script=/test/src/scripts/zfs.sh

@test "get pool size returns correct size" {
  source $titan_script
  function df() {
     echo " Avail"
     echo "48076M"
  }
  export -f df

  run get_pool_size /dir
  [ $status -eq 0 ]
  [ $output = "48044M" ]
}

@test "get pool size exits if df fails" {
  function docker() { /bin/true; }
  function jq() { /bin/true; }

  source $titan_script
  source $util_script

  function df() { /bin/false; }
  export -f df

  run get_pool_size /dir
  [ $status -eq 1 ]
}

@test "bind mounts fails if nsenter fails" {
  function docker() { /bin/true; }
  function jq() { /bin/true; }

  source $titan_script
  source $util_script

  function nsenter() { /bin/false; }
  export -f nsenter

  run bind_mounts pool /dir
  [ $status -eq 1 ]
}

@test "bind mounts succeeds if nsenter succeeds" {
  function docker() { /bin/true; }
  function jq() { /bin/true; }

  source $titan_script
  source $util_script

  function nsenter() { /bin/true; }
  export nsenter

  run bind_mounts pool /dir
  [ $status -eq 0 ]
}

@test "pool left intact if found" {
  function docker() { /bin/true; }
  function jq() { /bin/true; }

  source $titan_script
  source $zfs_script
  source $util_script

  function zpool() {
    [ $1 == "status" ]
  }
  export -f zpool

  run create_import_pool pool /dir /dir
  [ $status -eq 0 ]
}

@test "pool imported if cachefile is found" {
  function docker() { /bin/true; }
  function jq() { /bin/true; }

  source $titan_script
  source $zfs_script
  source $util_script

  function zpool() {
    [ $1 == "status" ] && return 1
    [ $1 == "import" ] && return 0
  }
  export -f zpool
  local dir=/tmp/test.$$
  mkdir -p $dir/pool
  mkdir -p $dir/mnt
  touch $dir/pool/cachefile

  run create_import_pool pool $dir/pool $dir/mnt
  rm -rf $dir
  [ $status -eq 0 ]
}

@test "pool create fails if import fails" {
  function docker() { /bin/true; }
  function jq() { /bin/true; }

  source $titan_script
  source $zfs_script
  source $util_script

  function zpool() {
    [ $1 == "status" ] && return 1
    [ $1 == "import" ] && return 1
  }
  export -f zpool
  local dir=/tmp/test.$$
  mkdir -p $dir/pool
  mkdir -p $dir/mnt
  touch $dir/pool/data

  run create_import_pool pool $dir/pool $dir/mnt
  rm -rf $dir
  [ $status -eq 1 ]
}

@test "pool create succeeds" {
  function docker() { /bin/true; }
  function jq() { /bin/true; }

  source $titan_script
  source $zfs_script
  source $util_script

  function zpool() {
    [ $1 == "status" ] && return 1
    [ $1 == "create" ] && return 0
  }
  function df() {
    echo "  Avail"
    echo "1234M"
  }
  function zfs() { /bin/true; }
  function truncate() { /bin/true; }
  export -f zpool df zfs truncate

  run create_import_pool pool /dir /dir
  [ $status -eq 0 ]
}

@test "pool create fails if zpool fails" {
  function docker() { /bin/true; }
  function jq() { /bin/true; }

  source $titan_script
  source $zfs_script
  source $util_script

  function zpool() {
    [ $1 == "status" ] && return 1
    [ $1 == "create" ] && return 1
  }
  function df() {
    echo "  Avail"
    echo "1234M"
  }
  function truncate() { /bin/true; }
  export -f zpool df

  run create_import_pool pool /dir /dir
  [ $status -eq 1 ]
}

@test "launch server succeeds" {
  function docker() { /bin/true; }
  function jq() { /bin/true; }

  source $titan_script
  source $util_script

  function mkdir() { /bin/true; }
  function rm() { /bin/true; }
  function docker() { /bin/true; }
  export -f mkdir rm docker

  run launch_server 1 2 3 4 5 6
  [ $status -eq 0 ]
}

@test "launch server fails if docker fails" {
  function docker() { /bin/true; }
  function jq() { /bin/true; }

  source $titan_script
  source $util_script

  function mkdir() { /bin/true; }
  function rm() { /bin/true; }
  function docker() { /bin/false; }
  export -f mkdir rm docker

  run launch_server 1 2 3 4 5 6
  [ $status -eq 1 ]
}

@test "unbind mounts fails if nsenter fails" {
  function docker() { /bin/true; }
  function jq() { /bin/true; }

  source $titan_script
  source $util_script

  function nsenter() { /bin/false; }
  export -f nsenter

  run unbind_mounts pool /dir
  [ $status -eq 1 ]
}

@test "unbind mounts succeeds if nsenter succeeds" {
  function docker() { /bin/true; }
  function jq() { /bin/true; }

  source $titan_script
  source $util_script

  function nsenter() { /bin/true; }
  export nsenter

  run unbind_mounts pool /dir
  [ $status -eq 0 ]
}
