val sparkV      = "3.5.5"
val hadoopV     = "3.4.1"

name            := "warc-for-spark"
organization    := "org.rubigdata"
version         := "0.1.0"
scalaVersion    := "2.12.10"

publishTo := Some("GitHub rubigdata Apache Maven Packages" at "https://maven.pkg.github.com/rubigdata/warc-for-spark")
publishMavenStyle := true
credentials += Credentials(
  "GitHub Package Registry",
  "maven.pkg.github.com",
  "rubigdata",
  System.getenv("GITHUB_TOKEN")
)

libraryDependencies ++= Seq(
  "org.apache.spark"     %% "spark-core"       % sparkV,
  "org.apache.spark"     %% "spark-sql"        % sparkV,
  "org.apache.hadoop"    %  "hadoop-client"    % hadoopV,
  "org.netpreserve"      % "jwarc"             % "0.31.1",
)
