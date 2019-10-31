package io.titandata.metadata.table

import org.jetbrains.exposed.sql.Table

class Repositories : Table() {
    val name = varchar("name", 64).primaryKey()
    val metadata = varchar("metadata", 8192)
    val activeSet = varchar("active_set", 32)
}