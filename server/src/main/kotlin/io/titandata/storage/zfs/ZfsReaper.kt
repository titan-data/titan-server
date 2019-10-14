/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.storage.zfs

/*
 * The reaper class is responsible for cleaning up repositories commits that can't be deleted because there are
 * dependent clones. Most of the time, we are just deleting snapshots - which can be handled by by using the
 * deferred destroy capability of ZFS. But when we
 */
class ZfsReaper {
}