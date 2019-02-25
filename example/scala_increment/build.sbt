name := "Increment App"

version := "1.0"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "2.4.0",
                            "com.ericbarnhill.niftijio" % "niftijio" % "1.0")
