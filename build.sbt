import Dependencies._

ThisBuild / scalaVersion     := "2.12.8"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

lazy val root = (project in file("."))
  .settings(
    name := "spark-ceph-connector",

    // https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-common
    // FIXME: The version should be 2.9.2 but temporarily use 2.9.1
    //   because there is no source code in the Maven repository
    libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.9.1",

    // https://mvnrepository.com/artifact/com.ceph/rados
    libraryDependencies += "com.ceph" % "rados" % "0.5.0",

    libraryDependencies += scalaTest % Test,

    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", xs@_*) => MergeStrategy.discard
      case x => MergeStrategy.first
    }
  )

// Uncomment the following for publishing to Sonatype.
// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for more detail.

ThisBuild / description := "spark-ceph-connector"
 ThisBuild / licenses    := List("Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt"))
 ThisBuild / homepage    := Some(url("https://github.com/shuuji3/spark-ceph-connector"))
 ThisBuild / scmInfo := Some(
   ScmInfo(
     url("https://github.com/shuuji3/spark-ceph-connector"),
     "scm:git@github.com:shuuji3/spark-ceph-connector.git"
   )
 )
 ThisBuild / developers := List(
   Developer(
     id    = "shuuji3",
     name  = "TAKAHASHI Shuuji",
     email = "shuuji3@gmail.com",
     url   = url("https://github.com/shuuji3")
   )
 )
// ThisBuild / pomIncludeRepository := { _ => false }
// ThisBuild / publishTo := {
//   val nexus = "https://oss.sonatype.org/"
//   if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
//   else Some("releases" at nexus + "service/local/staging/deploy/maven2")
// }
// ThisBuild / publishMavenStyle := true
