package io.titandata.metadata.table

import io.titandata.metadata.table.Repositories.primaryKey
import org.jetbrains.exposed.sql.ReferenceOption
import org.jetbrains.exposed.sql.Table

object Remotes : Table() {
    val name = Repositories.varchar("name", 64).primaryKey()
    val repo = Repositories.varchar("repositories", 64).references(Repositories.name, onDelete = ReferenceOption.CASCADE).primaryKey()
    val metadata = Repositories.varchar("metadata", 8192)
}
