package io.titandata.metadata.table

import io.titandata.metadata.MetadataProvider
import org.jetbrains.exposed.dao.UUIDTable

/*
 * Volume sets represent one or more volumes that are part of a repository. They always have unique UUIDs, and form
 * the basis of all persistent storage objects. Volume names are not unique, but the (VolumeSet, Volume) is. Commits
 * are made of volume sets, though it's volumes that are individually mounted. It's up to the storage provider to
 * determine how to use volumesets. They may simply be a way to name volumes, or they could correspond to a unique
 * value on disk.
 *
 * We do not maintain a foreign key relationship with the repository because they can be deleted asynchronously
 * after the repository has been deleted.
 */
object VolumeSets : UUIDTable("volume_sets") {
    val repo = varchar("repositories", 64)
    val state = enumerationByName("state", 16, MetadataProvider.VolumeState::class)
}
