import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
    kotlin("jvm")
    jacoco
    application
    id("com.github.johnrengelman.shadow") version("5.2.0")
}

val titanVersion: String by rootProject.extra
group = "io.titandata"
version = titanVersion

repositories {
    mavenLocal()
    mavenCentral()
    jcenter()
    maven("https://dl.bintray.com/kotlin/ktor")
    maven("https://dl.bintray.com/kotlin/kotlinx")
    maven("https://dl.bintray.com/kotlin/exposed")
    maven {
        name = "titan"
        url = uri("https://maven.titan-data.io")
    }
}

val ktorVersion = "1.2.6"

dependencies {
    compile(project(":client"))
    compile(kotlin("stdlib"))
    compile("io.ktor:ktor-server-cio:$ktorVersion")
    compile("io.ktor:ktor-gson:$ktorVersion")
    compile("ch.qos.logback:logback-classic:1.2.3")
    compile("com.google.code.gson:gson:2.8.6")
    compile("com.squareup.okhttp3:okhttp:4.2.2")
    compile("org.jetbrains.exposed:exposed:0.17.7")
    compile("org.postgresql:postgresql:42.2.9")
    compile("com.zaxxer:HikariCP:3.4.1")
    compile("io.kubernetes:client-java:7.0.0")

    // Remotes
    compile("io.titandata:remote-sdk:0.2.0")
    compile("io.titandata:nop-remote-server:0.2.0")
    compile("io.titandata:ssh-remote-server:0.2.1")
    compile("io.titandata:s3-remote-server:0.2.0")
    compile("io.titandata:s3web-remote-server:0.2.0")

    testCompile("com.h2database:h2:1.4.200")
    testCompile("io.ktor:ktor-server-test-host:$ktorVersion")
    testImplementation("io.kotlintest:kotlintest-runner-junit5:3.4.2")
    testImplementation("io.mockk:mockk:1.9.3")
    testImplementation("org.apache.commons:commons-text:1.8")
}

jacoco {
    toolVersion = "0.8.4"
}

application {
    mainClassName = "io.titandata.ApplicationKt"
}

tasks.withType<ShadowJar> {
    archiveFileName.set("titan-server.jar")
    mergeServiceFiles()
}

tasks.register("rebuild") {
    group = LifecycleBasePlugin.BUILD_GROUP
    description = "Fast rebuild of docker image"
}

tasks.register("publish") {
    group = "Publishing"
    description = "Publish build artifacts"
}

apply{ from("${project.projectDir}/gradle/ktlint.gradle.kts") }
apply{ from("${project.projectDir}/gradle/unitTest.gradle") }
apply{ from("${project.projectDir}/gradle/integrationTest.gradle") }
apply{ from("${project.projectDir}/gradle/docker.gradle.kts") }
apply{ from("${project.projectDir}/gradle/shell.gradle.kts") }
