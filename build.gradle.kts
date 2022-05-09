import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.6.20"
    id("com.github.johnrengelman.shadow") version "7.0.0"
}

group = "org.jdbc-wiql"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
    maven(url = "https://jitpack.io")
}

dependencies {
    implementation("org.slf4j:slf4j-simple:1.7.36")
    implementation("io.github.microutils:kotlin-logging:2.1.21")
    implementation("org.jetbrains.kotlin:kotlin-osgi-bundle:1.6.10")
    implementation("org.junit.jupiter:junit-jupiter:5.8.2")
    implementation("io.ktor:ktor-client-content-negotiation:2.0.1")
    implementation("io.ktor:ktor-serialization-gson:2.0.1")
    implementation("io.ktor:ktor-client-apache-jvm:2.0.1")
    implementation("io.ktor:ktor-client-core-jvm:2.0.1")
    implementation("io.ktor:ktor-client-cio-jvm:2.0.1")
    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
    kotlinOptions.apiVersion = "1.6"
}