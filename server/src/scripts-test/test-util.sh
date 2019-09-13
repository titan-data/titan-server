#!/usr/bin/env bats

util_script=/test/src/scripts/util.sh

@test "user defaults set correctly" {
  function docker() { /bin/true; }
  function jq() { /bin/true; }

   source $util_script
   [ $IDENTITY = "titan" ]
   [ $PORT = "5001" ]
   [ $IMAGE = "titan:latest" ]
}

@test "user overrides propagated correctly" {
  function docker() { /bin/true; }
  function jq() { /bin/true; }

   export TITAN_IDENTITY=test
   export TITAN_PORT=6001
   export TITAN_IMAGE=titandata/titan:test
   source $util_script
   [ $IDENTITY = "test" ]
   [ $PORT = "6001" ]
   [ $IMAGE = "titandata/titan:test" ]
}

@test "derived variables set correctly" {
  function docker() { /bin/true; }
  function jq() { echo "/path"; }

   source $util_script
   [ $POOL = "titan" ]
   [ $VOLUME = "titan-data" ]
   [ $BASE_DIR = "/var/lib/titan" ]
   [ $DATA_DIR = "/var/lib/titan/data" ]
   [ $INSTALL_DIR = "/var/lib/titan/data/install" ]
   [ $POOL_DIR = "/path/pool" ]
   [ $MNT_DIR = "/var/lib/titan/mnt" ]
   [ $SYSTEM_MODULES = "/var/lib/titan/system" ]
   [ $COMPILED_MODULES = "/var/lib/titan/data/modules" ]
}

@test "timestamp returns date output" {
  function docker() { /bin/true; }
  function jq() { /bin/true; }

  source $util_script
  function date() { echo "date"; }
  export -f date
  run timestamp
  [ $status -eq 0 ]
  [ "$output" = "date" ]
}

@test "log begin prints marker" {
  function docker() { /bin/true; }
  function jq() { /bin/true; }

  source $util_script
  function timestamp { echo "ts"; }
  export -f timestamp
  run log_begin
  [ $status -eq 0 ]
  [ "$output" = "ts TITAN BEGIN" ]
}

@test "log start prints marker" {
  function docker() { /bin/true; }
  function jq() { /bin/true; }

  source $util_script
  function timestamp { echo "ts"; }
  export -f timestamp
  run log_start "this is my message"
  [ $status -eq 0 ]
  [ "$output" = "ts TITAN START this is my message" ]
}

@test "log end prints marker" {
  function docker() { /bin/true; }
  function jq() { /bin/true; }

  source $util_script
  function timestamp { echo "ts"; }
  export -f timestamp
  run log_end
  [ $status -eq 0 ]
  [ "$output" = "ts TITAN END" ]
}

@test "log error exits program" {
  function docker() { /bin/true; }
  function jq() { /bin/true; }

  source $util_script
  function timestamp { echo "ts"; }
  export -f timestamp
  run log_error "this is my message"
  [ $status -eq 1 ]
  [ "$output" = "ts TITAN ERROR this is my message" ]
}
