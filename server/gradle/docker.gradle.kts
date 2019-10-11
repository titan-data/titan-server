val imageName = when(project.hasProperty("serverImageName")) {
    true -> project.property("serverImageName")
    false -> "titandata/titan"
}

var buildDockerServer = tasks.register<Exec>("buildDockerServer") {
    group = LifecycleBasePlugin.BUILD_GROUP
    description = "Build docker server image"
    commandLine("docker", "build", "--no-cache", "-t", "$imageName:latest", "-f", "${project.projectDir}/docker/server.Dockerfile", "${project.projectDir}")
    mustRunAfter(tasks.named("shadowJar"))
}

// Convenience function that doesn't do --no-cache for quick rebuilds (at risk of potentially stale data
var rebuildDockerServer = tasks.register<Exec>("rebuildDockerServer") {
    group = LifecycleBasePlugin.BUILD_GROUP
    description = "Build docker server image"
    commandLine("docker", "build", "-t", "$imageName:latest", "-f", "${project.projectDir}/docker/server.Dockerfile", "${project.projectDir}")
    mustRunAfter(tasks.named("shadowJar"))
}

var tagDockerServer = tasks.register<Exec>("tagDockerServer") {
    group = LifecycleBasePlugin.BUILD_GROUP
    description = "Tag docker server image with current version"
    commandLine("docker", "tag", "$imageName:latest", "$imageName:${project.version}")
    mustRunAfter(tasks.named("buildDockerServer"))
}

var tagLocalDockerServer = tasks.register<Exec>("tagLocalDockerServer") {
    group = LifecycleBasePlugin.BUILD_GROUP
    description = "Tag docker server image with current version"
    commandLine("docker", "tag", "$imageName:latest", "titan:latest")
    mustRunAfter(tasks.named("buildDockerServer"))
}

var buildSshServer = tasks.register<Exec>("buildSshTestServer") {
    group = LifecycleBasePlugin.BUILD_GROUP
    description = "Build SSH endtoend server image"
    commandLine("docker", "build", "-t", "sshtest:latest", "-f", "${project.projectDir}/docker/sshtest.Dockerfile", "${project.projectDir}")
}

tasks.named("assemble").configure {
    dependsOn(buildDockerServer)
    dependsOn(tagDockerServer)
    dependsOn(tagLocalDockerServer)
}

tasks.named("rebuild").configure {
    dependsOn(tasks.named("shadowJar"))
    dependsOn(rebuildDockerServer)
    dependsOn(tagLocalDockerServer)
}

tasks.named("endtoendTest").configure {
    dependsOn(buildSshServer)
}

var publishDockerVersion = tasks.register<Exec>("publishDockerVersion") {
    group = "Publishing"
    description = "Publish versioned docker server image to docker hub"
    commandLine("docker", "push", "$imageName:${project.version}")
    mustRunAfter(tasks.named("tagDockerServer"))
}

var publishDockerLatest = tasks.register<Exec>("publishDockerLatest") {
    group = "Publishing"
    description = "Publish latest docker server image to docker hub"
    commandLine("docker", "push", "$imageName:latest")
    mustRunAfter(tasks.named("publishDockerVersion"))
}

tasks.named("publish").configure {
    dependsOn(publishDockerVersion)
    dependsOn(publishDockerLatest)
}

