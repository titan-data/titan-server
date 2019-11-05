package io.titandata.metadata.table

import io.titandata.metadata.MetadataProvider
import io.titandata.metadata.table.Remotes.primaryKey
import io.titandata.metadata.table.Remotes.references
import org.jetbrains.exposed.sql.Table

/*
 * Volumes represent a single mount within a repository. They are grouped into VolumeSets, so that they are snapshotted
 * and cloned as a group. Volume names are not globally unique, but are unique within a given volumeset, so the
 * (volumeset, name) tuple uniquely identifies any volume in the system. Volumes can have user properties, which can
 * be used to store the path where the volume is supposed to be mounted or other useful mmetadata.
 *
 * Volumes have a foreign key relationship with volumesets, but we don't cascade on delete because we want to ensure
 * that all volumes are explicitly deleted prior to deleting the volumeset.
 */
object Volumes : Table("volumes") {
    val volumeSet = uuid("volume_set").references(VolumeSets.id).primaryKey()
    val name = varchar("name", 64).primaryKey()
    val metadata = varchar("metadata", 8192)
    val state = enumerationByName("state", 16, MetadataProvider.VolumeState::class)
}
