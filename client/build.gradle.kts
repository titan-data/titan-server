plugins {
    kotlin("jvm")
    `maven-publish`
}

val titanVersion: String by rootProject.extra

group = "io.titan-data.client"
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
            groupId = "io.titan-data"
            artifactId = "titan-client"

            from(components["java"])
        }
    }
}

val openapiVersion = "v4.0.2"

dependencies {
    compile(kotlin("stdlib"))
    compile("com.squareup.okhttp3:okhttp:3.14.2")
    compile("com.google.code.gson:gson:2.8.5")
    compile("software.amazon.awssdk:auth:2.7.33")
}
