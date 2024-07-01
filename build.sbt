val sparkV      = "3.5.0"
val hadoopV     = "3.3.6"

name            := "WARC4Spark"
organization    := "org.rubigdata.warc"
version         := sparkV
scalaVersion    := "2.12.10"

libraryDependencies ++= Seq(
  "org.apache.spark"     %% "spark-core"       % sparkV,
  "org.apache.spark"     %% "spark-sql"        % sparkV,
  "org.apache.hadoop"    %  "hadoop-client"    % hadoopV,
  "org.netpreserve"      % "jwarc"             % "0.30.0"
)
