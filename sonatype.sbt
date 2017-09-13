pomExtra in Global := {
  <url>github.com/CatalystCode/streaming-rss-html</url>
    <licenses>
      <license>
        <name>MIT</name>
        <url>https://opensource.org/licenses/MIT</url>
      </license>
    </licenses>
    <scm>
      <connection>scm:git:github.com/CatalystCode/streaming-rss-html</connection>
      <developerConnection>scm:git:git@github.com:CatalystCode/streaming-rss-html</developerConnection>
      <url>github.com/CatalystCode/streaming-rss-html</url>
    </scm>
    <developers>
      <developer>
        <id>jcjimenez</id>
        <name>Juan-Carlos Jimenez</name>
        <email>jc.jimenez@microsoft.com</email>
        <url>http://github.com/jcjimenez</url>
      </developer>
    </developers>
}

credentials += Credentials(
  "Sonatype Nexus Repository Manager",
  "oss.sonatype.org",
  System.getenv("SONATYPE_USER"),
  System.getenv("SONATYPE_PASSWORD"))

organizationName := "Partner Catalyst"
organizationHomepage := Some(url("https://github.com/CatalystCode"))

publishTo := {
  val isSnapshot = version.value.trim.endsWith("SNAPSHOT")
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot) Some("snapshots" at nexus + "content/repositories/snapshots")
  else            Some("releases" at nexus + "service/local/staging/deploy/maven2")
}

publishMavenStyle := true
publishArtifact in Test := false
useGpg := true
