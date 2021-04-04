# Google Cloud project and cluster information
PROJECT_NAME="ggcd-hadoop"
CLUSTER_NAME="hadoop-cluster"
REGION="europe-west1"
ZONE="europe-west1-b"
MACHINE_TYPE="n1-standard-4"
DISK_SIZE=50
DISK_TYPE="pd-ssd"
WORKERS=3
IMAGE_VERSION="2.0-debian10"
MAX_IDLE_SECONDS="7200s"
SCOPE="https://www.googleapis.com/auth/cloud-platform"


E_INVALID_ARGUMENTS=128
function argcEq () {
  if [ "$1" -ne "$2" ]; then
    # shellcheck disable=SC2059
    printf "$3\n"
    exit $E_INVALID_ARGUMENTS
fi
}

function argcGe () {
  if [ "$1" -lt "$2" ]; then
    # shellcheck disable=SC2059
    printf "$3\n"
    exit $E_INVALID_ARGUMENTS
  fi
}

case $1 in

  # Create cluster
  "create")
    argcEq "$#" 1 "Invalid input parameters, create requires no parameters"
    gcloud beta dataproc clusters create $CLUSTER_NAME --region $REGION --zone $ZONE --master-machine-type $MACHINE_TYPE --master-boot-disk-type $DISK_TYPE --master-boot-disk-size $DISK_SIZE --num-workers $WORKERS --worker-machine-type $MACHINE_TYPE --worker-boot-disk-type $DISK_TYPE --worker-boot-disk-size $DISK_SIZE --image-version $IMAGE_VERSION --max-idle $MAX_IDLE_SECONDS --scopes $SCOPE --project $PROJECT_NAME
    ;;

  # Stop cluster
  "stop")
    argcEq "$#" 1 "Invalid input parameters, stop requires no parameters"
    gcloud beta dataproc clusters stop $CLUSTER_NAME --region="$REGION"
    ;;

  # Start cluster
  "start")
    argcEq "$#" 1 "Invalid input parameters, start requires no parameters"
    gcloud beta dataproc clusters start $CLUSTER_NAME --region="$REGION"
    ;;

  # Delete cluster
  "delete")
    argcEq "$#" 1 "Invalid input parameters, delete requires no parameters"
    gcloud beta dataproc clusters delete $CLUSTER_NAME --region=$REGION
    ;;

  # Submit job
  "submit")
    argcGe "$#" 3 "Invalid input parameters, submit requires at least 2 parameters.\\nUsage: ./create.sh submit <JARS> <MAIN_CLASS> [JOB ARGUMENTS]"
    gcloud dataproc jobs submit hadoop --region=$REGION --cluster=$CLUSTER_NAME  --jars="$2" --class="$3" -- "${@:4}" --reducers=$WORKERS
    ;;

  # Add file to HDFS
  "hdfs_upload")
    argcEq "$#" 3 "Invalid input parameters, hdfs_upload requires 2 parameters.\nUsage: ./create.sh hdfs_upload <SRC> <DEST>"
    FILE="$(basename "$2")"
    gcloud compute scp --recurse --zone="$ZONE" "$2" "$CLUSTER_NAME"-m:"$FILE"
    gcloud compute ssh --zone="$ZONE" "$CLUSTER_NAME"-m --command="hdfs dfs -put $FILE $3; rm -r $FILE"
    ;;

  # Download files from HDFS
  "hdfs_download")
    argcEq "$#" 2 "Invalid input parameters, hdfs_download requires 1 parameters.\nUsage: ./create.sh hdfs_download <SRC>"
    FILE="$(basename "$2")"
    gcloud compute ssh --zone="$ZONE" "$CLUSTER_NAME"-m --command="rm -r $FILE; hdfs dfs -get $2 $FILE"
    gcloud compute scp --zone="$ZONE" "$CLUSTER_NAME"-m:"$FILE" "$FILE"
    ;;

  # Delete file from HDFS
  "hdfs_delete")
    argcEq "$#" 2 "Invalid input parameters, hdfs_delete requires 1 parameters.\nUsage: ./create.sh hdfs_delete <SRC>"
    gcloud compute ssh --zone="$ZONE" "$CLUSTER_NAME"-m --command="hdfs dfs -rm -R $2"
    ;;

esac