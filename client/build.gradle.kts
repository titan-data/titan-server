plugins {
    kotlin("jvm")
    `maven-publish`
}

val titanVersion: String by rootProject.extra
val mavenUrl = when(project.hasProperty("mavenUrl")) {
    true -> project.property("mavenUrl")
    false -> "https://maven.pkg.github.com/titan-data"
}
val mavenUser = when(project.hasProperty("mavenUser")) {
    true -> project.property("mavenUser").toString()
    false -> null
}
val mavenPassword = when(project.hasProperty("mavenPassword")) {
    true -> project.property("mavenPassword").toString()
    false -> null
}

group = "io.titandata"
version = titanVersion


java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
}

repositories {
    mavenCentral()
}

val jar by tasks.getting(Jar::class) {
    archiveBaseName.set("titan-client")
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            groupId = "io.titandata"
            artifactId = "titan-client"

            from(components["java"])
        }
    }

    repositories {
        maven {
            name = "titan"
            url = uri("$mavenUrl")
            credentials {
                username = mavenUser
                password = mavenPassword
            }
        }
    }
}

dependencies {
    compile(kotlin("stdlib"))
    compile("com.squareup.okhttp3:okhttp:3.14.2")
    compile("com.google.code.gson:gson:2.8.5")
    compile("software.amazon.awssdk:auth:2.7.33")
}
