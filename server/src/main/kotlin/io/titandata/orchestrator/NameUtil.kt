package io.titandata.orchestrator

import java.util.UUID

/**
 * Our object names are designed to be used within supported contexts, such as kubernetes and ZFS. This allows us to
 * name objects with matching names and not have to create a layer of indirection at the metadata layer. Note that
 * there are some names that don't have storage representation (such as remotes), but
 *
 * ZFS components can contain alphanumeric characters plus '_', '-', ':', and '.'. Components must be 255 characters
 * or less.
 *
 * Kubernetes names can contain alphanumeric characters plus '-' and '.'. Names can be up to 253 characters long. Note
 * that some kubernetes resources have additional restrictions, like what characters can be at the beginning and end.
 * The assumption is that in those cases we can augment the string (e.g. add leading text) without having to
 * introduce those restrictions into our core naming rules.
 *
 * For our names, we take the subset of those two groups (basically the kubernetes restrictions), but limit them to
 * 63 characters. 253 is just way more than we reasonably need, and by limiting it to 63 we can ensure that we can
 * concatenate multiple names and still remain under that 253 limit.
 */
class NameUtil {

    companion object {
        private val nameRegex = "^[a-zA-Z0-9\\-.]+$".toRegex()
        private val nameLimit = 63

        internal fun validateCommon(name: String, type: String) {
            if (!nameRegex.matches(name)) {
                throw IllegalArgumentException("invalid $type name, can only contain " +
                        "alphanumeric characters, '-', or '.'")
            }
            if (name.length > nameLimit) {
                throw IllegalArgumentException("invalid $type name, must be $nameLimit characters or less")
            }
            if (name.length == 0) {
                throw IllegalArgumentException("invalid $type name, cannot be empty")
            }
        }

        fun validateRepoName(repoName: String) {
            validateCommon(repoName, "repository")
        }

        fun validateCommitId(commitId: String) {
            validateCommon(commitId, "commit id")
        }

        fun validateRemoteName(remoteName: String) {
            validateCommon(remoteName, "remote")
        }

        fun validateVolumeName(volumeName: String) {
            validateCommon(volumeName, "volume")
        }

        fun validateOperationId(operationId: String) {
            try {
                UUID.fromString(operationId)
            } catch (e: IllegalArgumentException) {
                throw IllegalArgumentException("invalid operation ID, must be a UUID")
            }
        }
    }
}
