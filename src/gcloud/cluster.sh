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


# Input parameter should be action to perform upon cluster
#if test "$#" -ne 1; then
#    echo "Illegal number of parameters!"
#fi

case $1 in

  # Create cluster
  "create")
    gcloud beta dataproc clusters create $CLUSTER_NAME --region $REGION --zone $ZONE --master-machine-type $MACHINE_TYPE --master-boot-disk-type $DISK_TYPE --master-boot-disk-size $DISK_SIZE --num-workers $WORKERS --worker-machine-type $MACHINE_TYPE --worker-boot-disk-type $DISK_TYPE --worker-boot-disk-size $DISK_SIZE --image-version $IMAGE_VERSION --max-idle $MAX_IDLE_SECONDS --scopes $SCOPE --project $PROJECT_NAME
    ;;

  # Stop cluster
  "stop")
    gcloud beta dataproc clusters stop $CLUSTER_NAME --region="$REGION"
    ;;

  # Start cluster
  "start")
    gcloud beta dataproc clusters start $CLUSTER_NAME --region="$REGION"
    ;;

  # Delete cluster
  "delete")
    gcloud beta dataproc clusters delete $CLUSTER_NAME --region=$REGION
    ;;

  # Submit job
  "submit")
    gcloud dataproc jobs submit hadoop --region=$REGION --cluster=$CLUSTER_NAME  --jars="$2" --class="$3" -- "$4" "$5" "$6"
    ;;

  # Add file to HDFS
  "hdfs_upload")
    FILE="$(basename "$2")"
    gcloud compute scp --recurse --zone="$ZONE" "$2" "$CLUSTER_NAME"-m:"$FILE"
    gcloud compute ssh --zone="$ZONE" "$CLUSTER_NAME"-m --command="hdfs dfs -put $FILE /$FILE"
    ;;

  # Download files from HDFS
  "hdfs_download")
    FILE="$(basename "$2")"
    gcloud compute ssh --zone="$ZONE" "$CLUSTER_NAME"-m --command="rm -r $FILE; hdfs dfs -get $2 $FILE"
    gcloud compute scp --zone="$ZONE" "$CLUSTER_NAME"-m:"$FILE" "$FILE"
    ;;

  # Delete file from HDFS
  "hdfs_delete")
    gcloud compute ssh --zone="$ZONE" "$CLUSTER_NAME"-m --command="hdfs dfs -rm -R $2"
    ;;

esac