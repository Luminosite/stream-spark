name:="My Net Stream Testing"
version:="1.0"
scalaVersion:="2.10.4"
val hbaseVersion = "0.98.4-hadoop2"
libraryDependencies+="org.apache.spark"%"spark-streaming_2.10"%"1.5.2"
libraryDependencies += "org.apache.hbase" % "hbase-client" % hbaseVersion
libraryDependencies += "org.apache.hbase" % "hbase-common" % hbaseVersion
libraryDependencies += "org.apache.hbase" % "hbase-server" % hbaseVersion

libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.0"

//libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.9.0.0"
//libraryDependencies += "org.apache.kafka" % "kafka_2.10" % "0.9.0.0"

// Use local repositories by default
resolvers ++= Seq(
  Resolver.defaultLocal,
  Resolver.mavenLocal,
  // make sure default maven local repository is added... Resolver.mavenLocal has bugs.
  "Local Maven Repository" at Path.userHome.asFile.toURI.toURL + "/.m2/repository",
  // For Typesafe goodies, if not available through maven
  // "Typesafe" at "http://repo.typesafe.com/typesafe/releases",
  // For Spark development versions, if you don't want to build spark yourself
  "Apache Staging" at "https://repository.apache.org/content/repositories/staging/",
  "PayPal Nexus releases" at "http://nexus.paypal.com/nexus/content/repositories/releases",
  "PayPal Nexus snapshots" at "http://nexus.paypal.com/nexus/content/repositories/snapshots",
  "Artima Maven Repository" at "http://repo.artima.com/releases"
)