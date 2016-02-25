import sbt._
import sbt.Keys._
import com.typesafe.sbt.SbtMultiJvm
import com.typesafe.sbt.SbtMultiJvm.MultiJvmKeys.MultiJvm
import sbtfilter.Plugin.FilterKeys._
import scoverage.ScoverageSbtPlugin._

object Build extends sbt.Build {

  lazy val akka_persistence_hbase = Project("akka-persistence-hbase", file("."))
    .settings(basicSettings: _*)
    .settings(Formatting.settings: _*)
    .settings(Formatting.buildFileSettings: _*)
    .settings(releaseSettings: _*)
    .settings(sbtrelease.ReleasePlugin.releaseSettings: _*)
    .settings(libraryDependencies ++= Dependencies.basic)
    .settings(instrumentSettings: _*)
    .settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)
    .settings(multiJvmSettings: _*)
    .settings(unmanagedSourceDirectories in Test += baseDirectory.value / "multi-jvm/scala")
    .configs(MultiJvm)

  lazy val basicSettings = Seq(
    organization := "pl.project13.scala",
    version := "0.5.0-SNAPSHOT",
    scalaVersion := "2.11.7",
    scalacOptions ++= Seq("-unchecked", "-deprecation"),
    javacOptions ++= Seq("-source", "1.6", "-target", "1.6", "-g"),
    resolvers ++= Seq(
      "Sonatype OSS Releases" at "https://oss.sonatype.org/content/repositories/releases",
      "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
      "Typesafe repo" at "http://repo.typesafe.com/typesafe/releases/",
      "krasserm at bintray" at "http://dl.bintray.com/krasserm/maven"))

  // Todo rewrite sbt-avro to compile in Test phase.
  lazy val releaseSettings = Seq(
    publishTo := {
      val nexus = "https://oss.sonatype.org/"
      if (version.value.trim.endsWith("SNAPSHOT"))
        Some("snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("releases" at nexus + "service/local/staging/deploy/maven2")
    },
    publishMavenStyle := true,
    publishArtifact in Test := false,
    pomIncludeRepository := { (repo: MavenRepository) => false },
    pomExtra := pomXml)

  lazy val pomXml =
    (<url>http://github.com/ktoso/akka-persistence-hbase</url>
     <licenses>
       <license>
         <name>Apache 2 License</name>
         <url>http://www.apache.org/licenses/LICENSE-2.0.html</url>
         <distribution>repo</distribution>
       </license>
     </licenses>
     <scm>
       <url>git@github.com:ktoso/akka-persistence-hbase.git</url>
       <connection>scm:git:git@github.com:ktoso/akka-persistence-hbase.git</connection>
     </scm>
     <developers>
       <developer>
         <id>ktoso</id>
         <name>Konrad 'ktoso' Malawski</name>
         <url>http://blog.project13.pl</url>
       </developer>
     </developers>
     <parent>
       <groupId>org.sonatype.oss</groupId>
       <artifactId>oss-parent</artifactId>
       <version>7</version>
     </parent>)

  def multiJvmSettings = SbtMultiJvm.multiJvmSettings ++ Seq(
    compile in MultiJvm <<= (compile in MultiJvm) triggeredBy (compile in Test),
    parallelExecution in Test := false,
    executeTests in Test <<= (executeTests in Test, executeTests in MultiJvm) map {
      case (testResults, multiNodeResults) =>
        val overall =
          if (testResults.overall.id < multiNodeResults.overall.id)
            multiNodeResults.overall
          else
            testResults.overall
        Tests.Output(overall,
          testResults.events ++ multiNodeResults.events,
          testResults.summaries ++ multiNodeResults.summaries)
    })

  lazy val noPublishing = Seq(
    publish := (),
    publishLocal := (),
    // required until these tickets are closed https://github.com/sbt/sbt-pgp/issues/42,
    // https://github.com/sbt/sbt-pgp/issues/36
    publishTo := None)

}

object Dependencies {
  val SLF4J_VERSION = "1.7.7"
  val AKKA_VERSION = "2.4.2"
  val HADOOP_VERSION = "2.5.0"
  val HBASE_VERSION = "0.98.9-hadoop2"

  val akka = Seq(
    "com.typesafe.akka" %% "akka-actor" % AKKA_VERSION,
    "com.typesafe.akka" %% "akka-contrib" % AKKA_VERSION,
    "com.typesafe.akka" %% "akka-persistence" % AKKA_VERSION exclude ("org.iq80.leveldb", "leveldb"),
    "com.typesafe.akka" %% "akka-slf4j" % AKKA_VERSION,
    "com.typesafe.akka" %% "akka-testkit" % AKKA_VERSION % Test,
    "com.typesafe.akka" %% "akka-multi-node-testkit" % AKKA_VERSION % Test,
    "org.iq80.leveldb" % "leveldb" % "0.7" % Runtime,
    "org.fusesource.leveldbjni" % "leveldbjni-all" % "1.8" % Runtime)

  val hadoop = Seq(
    "org.apache.hadoop" % "hadoop-common" % HADOOP_VERSION,
    "org.apache.hadoop" % "hadoop-hdfs" % HADOOP_VERSION)

  val hbase = Seq(
    "org.apache.hbase" % "hbase-common" % HBASE_VERSION % "compile",
    "org.apache.hbase" % "hbase-client" % HBASE_VERSION % "compile",
    "org.hbase" % "asynchbase" % "1.5.0" exclude ("org.slf4j", "log4j-over-slf4j") exclude ("org.slf4j", "jcl-over-slf4j"))

  val log = Seq(
    "org.slf4j" % "slf4j-api" % SLF4J_VERSION,
    "org.slf4j" % "jcl-over-slf4j" % SLF4J_VERSION,
    "org.slf4j" % "log4j-over-slf4j" % SLF4J_VERSION,
    "ch.qos.logback" % "logback-classic" % "1.1.2")

  val test = Seq(
    "org.scalamock" %% "scalamock-scalatest-support" % "3.2.1" % Test,
    "org.scalatest" %% "scalatest" % "2.2.4" % Test)

  val guava = Seq(
    "com.google.guava" % "guava" % "18.0")

  val basic: Seq[sbt.ModuleID] = akka ++ hadoop ++ hbase ++ log ++ guava ++ test

}

object Formatting {
  import com.typesafe.sbt.SbtScalariform
  import com.typesafe.sbt.SbtScalariform.ScalariformKeys
  import ScalariformKeys._

  val BuildConfig = config("build") extend Compile
  val BuildSbtConfig = config("buildsbt") extend Compile

  // invoke: build:scalariformFormat
  val buildFileSettings: Seq[Setting[_]] = SbtScalariform.noConfigScalariformSettings ++
    inConfig(BuildConfig)(SbtScalariform.configScalariformSettings) ++
    inConfig(BuildSbtConfig)(SbtScalariform.configScalariformSettings) ++ Seq(
      scalaSource in BuildConfig := baseDirectory.value / "project",
      scalaSource in BuildSbtConfig := baseDirectory.value,
      includeFilter in (BuildConfig, format) := ("*.scala": FileFilter),
      includeFilter in (BuildSbtConfig, format) := ("*.sbt": FileFilter),
      format in BuildConfig := {
        val x = (format in BuildSbtConfig).value
        (format in BuildConfig).value
      },
      ScalariformKeys.preferences in BuildConfig := formattingPreferences,
      ScalariformKeys.preferences in BuildSbtConfig := formattingPreferences)

  val settings = SbtScalariform.scalariformSettings ++ Seq(
    ScalariformKeys.preferences in Compile := formattingPreferences,
    ScalariformKeys.preferences in Test := formattingPreferences)

  val formattingPreferences = {
    import scalariform.formatter.preferences._
    FormattingPreferences()
      .setPreference(RewriteArrowSymbols, false)
      .setPreference(AlignParameters, true)
      .setPreference(AlignSingleLineCaseStatements, true)
      .setPreference(DoubleIndentClassDeclaration, true)
      .setPreference(IndentSpaces, 2)
  }
}


