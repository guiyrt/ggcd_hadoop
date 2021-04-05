# Any command failure will stop execution
set -e

# Check if got correct number of inputs
if [ "$#" -ne 3 ]; then
  printf "Wrong number of arguments!\nUsage: ./run.sh <title.basics FILE> <title.ratings FILE> <OUTPUT DIR>"
  exit 128 # 128 is invalid arguments error
fi

# TODO: VERIFY IF INPUTS ARE FILE FILE AND DIR

CLUSTER=src/gcloud/cluster.sh
JAR=target/ggcd_hadoop-1.0.jar
BASICS="$(basename "$1")"
RATINGS="$(basename "$2")"
OUTPUT_DIR="$(echo "$3" | sed 's:/*$::')"

# Compile and generate JAR
mvn package

# Create cluster
./$CLUSTER create

# Upload files to HDFS
./$CLUSTER hdfs_upload schemas /
./$CLUSTER hdfs_upload "$1" /"$BASICS"
./$CLUSTER hdfs_upload "$2" /"$RATINGS"

# Run jobs
./$CLUSTER submit "$JAR" Jobs.BasicsRatingsParquet --input=hdfs:///"$BASICS" --ratings=hdfs:///"$RATINGS" --schemas=hdfs:///schemas --output=hdfs:///basicsRatingsOutput
./$CLUSTER submit "$JAR" Jobs.YearMovie --input=hdfs:///basicsRatingsOutput --schemas=hdfs:///schemas --output=hdfs:///yearMovieOutput
./$CLUSTER submit "$JAR" Jobs.MovieSuggestion --input=hdfs:///basicsRatingsOutput --schemas=hdfs:///schemas --output=hdfs:///movieSuggestionOutput
./$CLUSTER submit "$JAR" Jobs.ParquetToJson --input=hdfs:///yearMovieOutput --output=hdfs:///yearMovieJson --firstAsId=true
./$CLUSTER submit "$JAR" Jobs.ParquetToJson --input=hdfs:///basicsRatingsOutput --output=hdfs:///basicsRatingsJson

# Retrieve output
./$CLUSTER hdfs_download /basicsRatingsOutput "$OUTPUT_DIR"
./$CLUSTER hdfs_download /yearMovieOutput "$OUTPUT_DIR"
./$CLUSTER hdfs_download /movieSuggestionOutput "$OUTPUT_DIR"
./$CLUSTER hdfs_download /yearMovieJson "$OUTPUT_DIR"
./$CLUSTER hdfs_download /basicsRatingsJson "$OUTPUT_DIR"

# Delete cluster
./$CLUSTER delete -y