plugins {
    kotlin("jvm")
    `maven-publish`
}

group = "com.delphix.sdk"
version = "0.1.0"

repositories {
    mavenCentral()
    jcenter()
    maven("https://dl.bintray.com/kotlin/kotlinx")
}

val jar by tasks.getting(Jar::class) {
    archiveBaseName.set("engine-sdk")
}

dependencies {
    compile(kotlin("stdlib"))
    compile(kotlin("reflect"))
    compile("org.json:json:20190722")
    compile("com.squareup.okhttp3:okhttp:3.14.2")
}
