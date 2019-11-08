package io.titandata.orchestrator

class NameUtil {

    companion object {
        private val nameRegex = "^[a-zA-Z0-9_\\-:.]+$".toRegex()

        fun validateRepoName(repoName: String) {
            if (!nameRegex.matches(repoName)) {
                throw IllegalArgumentException("invalid repository name, can only contain " +
                        "alphanumeric characters, '-', ':', '.', or '_'")
            }
            if (repoName.length > 64) {
                throw IllegalArgumentException("invalid repository name, must be 64 characters or less")
            }
        }

        fun validateCommitId(commitId: String) {
            if (!nameRegex.matches(commitId)) {
                throw IllegalArgumentException("invalid commit id, can only contain " +
                        "alphanumeric characters, '-', ':', '.', or '_'")
            }
            if (commitId.length > 64) {
                throw IllegalArgumentException("invalid commit id, must be 64 characters or less")
            }
        }

        fun validateRemoteName(repoName: String) {
            if (!nameRegex.matches(repoName)) {
                throw IllegalArgumentException("invalid remote name, can only contain " +
                        "alphanumeric characters, '-', ':', '.', or '_'")
            }
            if (repoName.length > 64) {
                throw IllegalArgumentException("invalid remote name, must be 64 characters or less")
            }
        }

        fun validateVolumeName(volumeName: String) {
            if (!nameRegex.matches(volumeName)) {
                throw IllegalArgumentException("invalid volume name, can only contain " +
                        "alphanumeric characters, '-', ':', '.', or '_'")
            }
            if (volumeName.length > 64) {
                throw IllegalArgumentException("invalid volume name, must be 64 characters or less")
            }
            if (volumeName.startsWith("_")) {
                throw IllegalArgumentException("volume names cannot start with '_'")
            }
        }
    }
}
