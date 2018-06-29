import sbt.Keys.libraryDependencies

lazy val sparkDemo = (project in file(".")).settings(
  Seq(
    name := "spark-demo",
    version := "1.0.0",
    scalaVersion := "2.11.8",
    resolvers := Seq(Resolver.mavenLocal),
    libraryDependencies := Seq(
      "org.apache.spark" %% "spark-core"   % "2.3.1"
    ),
    mainClass in (Compile, run) := Some("com.sparkdemo.Start")
  )
)


