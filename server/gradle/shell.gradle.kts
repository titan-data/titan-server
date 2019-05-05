var batsVersion = "v1.1.0"

var testShell = tasks.register<Exec>("testShell") {
    group = LifecycleBasePlugin.VERIFICATION_GROUP
    description = "Compile ZFS from default kernel version"
    var args = mutableListOf("docker", "run", "--rm", "-v", "${project.projectDir}:/test",
            "bats/bats:${batsVersion}")
    for (file in fileTree("${project.projectDir}/src/scripts-test")) {
        if (file.isFile()) {
            args.add("/test/src/scripts-test/${file.name}")
        }
    }
    commandLine(args)
}

tasks.named("check").configure {
    dependsOn(testShell)
}
