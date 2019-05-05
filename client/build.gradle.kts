plugins {
    kotlin("jvm")
    `maven-publish`
}

val titanVersion: String by rootProject.extra
val artifactoryUser: String = System.getProperty("ARTIFACTORY_USERNAME", "none")
val artifactoryPassword: String = System.getProperty("ARTIFACTORY_PASSWORD", "none")
var artifactoryUrl: String = System.getProperty("ARTIFACTORY_URL", "http://artifactory.delphix.com/artifactory/titan-gradle")

group = "com.delphix.titan.client"
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
            groupId = "com.delphix.titan"
            artifactId = "titan-client"

            from(components["java"])
        }
    }

    repositories {
        maven {
            credentials {
                username = artifactoryUser
                password = artifactoryPassword
            }
            url = uri(artifactoryUrl)
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
