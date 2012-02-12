name := "zookeeper-client"

version := "2.0.0_BT-1.1"

organization := "com.twitter"

scalaVersion := "2.9.1"

resolvers ++= Seq(
  "repo1" at "http://repo1.maven.org/maven2/",
  "jboss-repo" at "http://repository.jboss.org/maven2/",
  "apache" at "http://people.apache.org/repo/m2-ibiblio-rsync-repository/"
)

libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api" % "1.6.4",
  "org.slf4j" % "log4j-over-slf4j" % "1.6.4",
  "org.scala-tools.testing" % "specs_2.9.1" % "1.6.9" % "test",
  "junit" % "junit" % "4.8.2" % "test"
)

libraryDependencies += 
  "org.apache.zookeeper" % "zookeeper" % "3.3.4" exclude("log4j", "log4j") exclude("javax.jms", "jms") exclude("com.sun.jdmk", "jmxtools") exclude("com.sun.jmx", "jmxri") exclude("junit", "junit")

licenses += ("Apache 2", url("http://www.apache.org/licenses/LICENSE-2.0.txt</url>"))
