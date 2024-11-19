plugins {
    kotlin("jvm") version "1.9.22"
}

group = "org.opendc"
version = "3.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(19)
}