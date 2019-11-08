package io.titandata.metadata.table

import io.titandata.metadata.MetadataProvider
import io.titandata.metadata.table.Remotes.references
import io.titandata.models.Operation
import org.jetbrains.exposed.dao.IntIdTable
import org.jetbrains.exposed.sql.ReferenceOption

/*
 * Operations are additional metadata that can be associated with a volume set while an operation (push or pull)
 * is ongoing.
 */
object Operations : IntIdTable("commits") {
    val volumeSet = uuid("volume_set").references(VolumeSets.id, onDelete = ReferenceOption.CASCADE).primaryKey()
    val repo = varchar("repo", 64)
    val metadataOnly = bool("metadata_only")
    val remoteParameters = varchar("remote_parameters", 8192)
    val remote = varchar("remote", 64)
    val commitId = varchar("commit_id", 64)
    val type = enumerationByName("type", 4, Operation.Type::class)
    val state = enumerationByName("type", 16, Operation.State::class)
}
