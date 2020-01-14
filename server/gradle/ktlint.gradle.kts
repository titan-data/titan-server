val ktlint by configurations.creating

dependencies {
    ktlint("com.pinterest:ktlint:0.36.0")
}

var ktlintTask = tasks.register<JavaExec>("ktlint") {
    group = LifecycleBasePlugin.VERIFICATION_GROUP
    description = "Check Kotlin code style"
    classpath = ktlint
    main = "com.pinterest.ktlint.Main"
    args("src/main/**/*.kt", "src/test/**/*.kt",
        "src/integration-test/**/*.kt")
}

tasks.register<JavaExec>("ktlintFormat") {
    group = LifecycleBasePlugin.VERIFICATION_GROUP
    description = "Fix Kotlin code style deviations"
    classpath = ktlint
    main = "com.pinterest.ktlint.Main"
    args("-F", "src/main/**/*.kt", "src/test/**/*.kt",
        "src/integration-test/**/*.kt")
}

tasks.named("check").get().dependsOn(ktlintTask)
tasks.named("style").get().dependsOn(ktlintTask)
