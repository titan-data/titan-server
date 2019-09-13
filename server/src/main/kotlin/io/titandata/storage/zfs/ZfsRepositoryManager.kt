/*
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */

package io.titandata.storage.zfs

import io.titandata.exception.CommandException
import io.titandata.exception.InvalidStateException
import io.titandata.models.Remote
import io.titandata.models.Repository
import com.google.gson.reflect.TypeToken

/**
 * All repositories are placed under the "<pool>/repo" dataset. This is done to provide additional
 * ZFS namespace for titan (such as deathrow for cleaning commits). Within each repo, there can be
 * any number of child datasets, each named by a GUID, that can contain snapshots. Only one of
 * these can be active at any time, however, and is noted by the "com.delphix.titan:active"
 * property on the root repo dataset. Within each repo dataset there can be any number of named
 * volumes, and snapshots are taken at the root of each repo dataset. The result is a hierarchy as
 * such:
 *
 *      (pool)
 *      (pool)/repo
 *      (pool)/repo/(name)
 *      (pool)/repo/(name)/(guid)
 *      (pool)/repo/(name)/(guid)@(commit)
 *      (pool)/repo/(name)/(guid)/(volume)@(commit)
 *
 * With the exception of repo-wide operations (create, delete, log, etc), every operation will
 * first look up the current active dataset, and then perform the requisite actions (snapshot,
 * mount, unmount, etc) on that dataset.
 *
 * The storage provider also provides metadata storage by setting JSON string properties at both
 * the repo and snapshot level. This metadata is entirely under the control of the consumer,
 * with the exception of the 'timestampProperty' parameter. When creating a new commit, the
 * provider will fill in the timestamp with the exact creation time to ensure the most
 * accurate timestamp. This is stored in metadata because when we pull a commit down from a remote
 * server, we want to use the timestamp in the metadata, not when the snapshot was created.
 */
class ZfsRepositoryManager(val provider: ZfsStorageProvider) {

    private val poolName = provider.poolName
    private val METADATA_PROP = provider.METADATA_PROP
    private val ACTIVE_PROP = provider.ACTIVE_PROP
    private val REMOTES_PROP = provider.REMOTES_PROP
    private val INITIAL_COMMIT = provider.INITIAL_COMMIT

    /**
     * Parse the name and metadata for a repository, based on 'zfs list' output. This is invoked
     * when listing and fetching repositories. It assumes that there are only two fields being
     * listed, the name and the metadata. It will parse the metadata as JSON, throwing an error
     * if it's in an invalid format.
     *
     * To keep the listRepositories() code simple, this returns null when given just a pool name.
     * This is because 'zfs list -d 1' just limits the depth, and will still print the dataset
     * itself (the pool in this case).
     */
    fun parseRepository(line: String): Repository? {
        // Metadata must be last since it can contain tabs
        val regex = "^$poolName/repo/([^/\t]+)\t(.*)$".toRegex(RegexOption.MULTILINE)
        val result = regex.find(line) ?: return null
        val name = result.groupValues.get(1)
        val metadata = result.groupValues.get(2)

        return Repository(name = name, properties = provider.parseMetadata(metadata))
    }

    /**
     * Create a new repository. This involves three steps:
     *
     *  1. Create a new root repo dataset, with the com.delphix.titan:active property pointing to
     *     a new GUID.
     *  2. Create a new dataset with that GUID beneath the root repo
     *  3. Create a default snapshot named 'initial'
     *
     * The third step simplifies subsequent operations by always having a point to clone from,
     * even when no commits exist, so that we don't have to special case creating new datsets.
     */
    fun createRepository(repo: Repository) {
        provider.validateRepositoryName(repo.name)

        try {
            val guid = provider.generator.get()
            val json = provider.gson.toJson(repo.properties)
            val name = repo.name
            provider.executor.exec("zfs", "create", "-o", "mountpoint=legacy", "-o",
                    "$ACTIVE_PROP=$guid", "-o", "$METADATA_PROP=$json", "$poolName/repo/$name")
            provider.executor.exec("zfs", "create", "$poolName/repo/$name/$guid")

            // We always create a snapshot knows as "initial" that can be used to clone an empty
            // dataset. This keeps other code simple to avoid having to special-case an initial
            // clone
            provider.executor.exec("zfs", "snapshot", "-o", "$METADATA_PROP={}",
                    "$poolName/repo/$name/$guid@$INITIAL_COMMIT")
        } catch (e: CommandException) {
            provider.checkRepositoryExists(e, repo.name)
        }
    }

    /**
     * List all repositories. We use the '-d 1' option to ZFS to only list direct descendants
     * of the configured pool. Each response is parsed by the parseRepository() method.
     */
    fun listRepositories(): List<Repository> {
        val output = provider.executor.exec("zfs", "list", "-Ho", "name,$METADATA_PROP",
                "-d", "1", "$poolName/repo")

        val repos = mutableListOf<Repository>()
        for (line in output.lines()) {
            if (line != "") {
                val repo = parseRepository(line)
                if (repo != null) {
                    repos.add(repo)
                }
            }
        }
        return repos
    }

    /**
     * Get a single repository. This will throw a NoSuchObjectException if the repository isn't
     * found. We choose the route of throwing an exception instead of an optional null because
     * this will be called frequently in the context where we just want to generate a 404 exception,
     * and forcing all consumers to check and throw similar errors seems unnecessary.
     */
    fun getRepository(name: String): Repository {
        provider.validateRepositoryName(name)
        try {
            val output = provider.executor.exec("zfs", "list", "-Ho", "name,$METADATA_PROP",
                    "$poolName/repo/$name")
            val repo = parseRepository(output)
            if (repo == null) {
                throw InvalidStateException("failed to parse state for repository '$name'")
            }
            return repo
        } catch (e: CommandException) {
            provider.checkNoSuchRepository(e, name)
            throw e
        }
    }

    /**
     * Update a repository. The most common operation is to simply update the metadata, but this
     * can also be used to rename a repository.
     */
    fun updateRepository(name: String, repo: Repository) {
        provider.validateRepositoryName(name)
        provider.validateRepositoryName(repo.name)
        try {
            val json = provider.gson.toJson(repo.properties)
            provider.executor.exec("zfs", "set", "$METADATA_PROP=$json", "$poolName/repo/$name")
            if (name != repo.name) {
                provider.executor.exec("zfs", "rename", "$poolName/repo/$name",
                        "$poolName/repo/${repo.name}")
            }
        } catch (e: CommandException) {
            provider.checkNoSuchRepository(e, name)
            provider.checkRepositoryExists(e, repo.name)
            throw e
        }
    }

    /**
     * Delete a repository. We use the '-R' flag to 'zfs destroy' to destroy all clones in the
     * appropriate order.
     *
     * TODO remove all directories underneath the mountpoint
     */
    fun deleteRepository(name: String) {
        provider.validateRepositoryName(name)
        try {
            provider.executor.exec("zfs", "destroy", "-R", "$poolName/repo/$name")
        } catch (e: CommandException) {
            provider.checkNoSuchRepository(e, name)
            throw e
        }

        // Try to delete the directory, but it may not exist if no volumes have been created
        try {
            provider.executor.exec("rmdir", provider.getMountpoint(name))
        } catch (e: CommandException) {
            if (!e.output.contains("No such file or directory")) {
                throw e
            }
        }
    }

    /**
     * Get remotes for the given repository. These are stored as a separate property on the
     * root repo dataset. Because remotes leverage polymorphism to convert between types, this
     * requires a bit more gson gymnastics.
     */
    fun getRemotes(repo: String): List<Remote> {
        provider.validateRepositoryName(repo)
        try {
            val output = provider.executor.exec("zfs", "list", "-Ho", "$REMOTES_PROP",
                    "$poolName/repo/$repo").trim()
            if (output == "-") {
                return listOf()
            }
            val listType = object : TypeToken<List<Remote>>() { }.type
            return provider.gson.fromJson(output, listType)
        } catch (e: CommandException) {
            provider.checkNoSuchRepository(e, repo)
            throw e
        }
    }

    /**
     * Update the remotes for a given repository.
     */
    fun updateRemotes(repo: String, remotes: List<Remote>) {
        provider.validateRepositoryName(repo)
        try {
            val json = provider.gson.toJson(remotes)
            provider.executor.exec("zfs", "set", "$REMOTES_PROP=$json", "$poolName/repo/$repo")
        } catch (e: CommandException) {
            provider.checkNoSuchRepository(e, repo)
            throw e
        }
    }
}
