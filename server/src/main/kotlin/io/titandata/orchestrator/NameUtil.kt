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

        fun validateRemoteName(repoName: String) {
            if (!nameRegex.matches(repoName)) {
                throw IllegalArgumentException("invalid remote name, can only contain " +
                        "alphanumeric characters, '-', ':', '.', or '_'")
            }
            if (repoName.length > 64) {
                throw IllegalArgumentException("invalid remote name, must be 64 characters or less")
            }
        }
    }
}
