BASE_DIR=/user/vagrant/hive/stackoverflow

dirs=("users" "posts" "comments" "tags" "votes" "badges" "postlinks")
for i in "${dirs[@]}"
do
    hadoop fs -mkdir -p ${BASE_DIR}/${i}
    hadoop fs -mv output/${i}*.avro ${BASE_DIR}/${i}/
done
