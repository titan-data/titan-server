#
# Copyright The Titan Project Contributors.
#

#
# Primary configuration variables to control how the server is launched. The IDENTITY variable
# controls the name of the storage pool, data volume, base directory, and other configuration
# used by the remainder of the scripts. Combined with the PORT variable, it allows for parallel
# titan instances to run, such as when running end to end tests with an existing titan
# installation. The
#
IDENTITY=${TITAN_IDENTITY:-titan}
PORT=${TITAN_PORT:-5001}
IMAGE=${TITAN_IMAGE:-titan:latest}

echo "=== User configuration ==="
echo "IDENTITY = $IDENTITY"
echo "PORT = $PORT"
echo "IMAGE = $IMAGE"

#
# Secondary configuration variables that are derived from the primary configuration variables.
# There should not be a need to override these variables.
#
POOL=$IDENTITY
VOLUME=$IDENTITY-data
BASE_DIR=/var/lib/$IDENTITY
DATA_DIR=$BASE_DIR/data
INSTALL_DIR=$DATA_DIR/install
POOL_DIR=$(docker volume inspect $VOLUME | jq -r .[0].Mountpoint)/pool
MNT_DIR=$BASE_DIR/mnt
SYSTEM_MODULES=$BASE_DIR/system
COMPILED_MODULES=$DATA_DIR/modules
KERNEL_RELEASE=$(uname -r)

echo "=== Static configuration ==="

echo "POOL = $POOL"
echo "VOLUME = $VOLUME"
echo "BASE_DIR = $BASE_DIR"
echo "INSTALL_DIR = $INSTALL_DIR"
echo "DATA_DIR = $DATA_DIR"
echo "POOL_DIR = $POOL_DIR"
echo "MNT_DIR = $MNT_DIR"
echo "SYSTEM_MODULES = $SYSTEM_MODULES"
echo "COMPILED_MODULES = $COMPILED_MODULES"
echo "KERNEL_RELEASE = $KERNEL_RELEASE"

#
# Primary delimiter for log messages. Lines that contain this delimiter (after the timestamp)
# will be processed by the CLI during log scanning.
#
log_delimiter=TITAN

#
# Return a timestamp to use in log messages and elsewhere.
#
function timestamp {
  date +"%F_%T"
}

#
# Log the beginning of the launch sequence. The CLi will search for the last indication of this
# message, and then consume all log messages until it sees an error or the launcher has started
# sucessfully.
#
function log_begin {
  local ts=$(timestamp)
  echo "$ts $log_delimiter BEGIN"
}

#
# Log the beginning of a step. This will be picked up by the CLI and displayed while the
# subsequent code is running, when a titan end message is received, then the cli will display
# the step as completed and move on.
#
function log_start {
  local ts=$(timestamp)
  local msg=$1
  echo "$ts $log_delimiter START $msg"
}

#
# Log the end of a step. This will indicate to the CLI that it should stop waiting for the previous
# log_begin() to complete, and move on to the next step.
#
function log_end {
  local ts=$(timestamp)
  echo "$ts $log_delimiter END"
}

#
# Log the end of the launch sequence. The will indicate to the CLI that is should stop waiting for
# any further log_start()
#
function log_finished {
  local ts=$(timestamp)
  echo "$ts $log_delimiter FINISHED"
}

#
# Log an error. This will automatically exit the container with an error code, and the CLI will
# display the error prior to failing the command.
#
function log_error {
  local ts=$(timestamp)
  local msg=$1
  echo "$ts $log_delimiter ERROR $msg"
  exit 1
}
