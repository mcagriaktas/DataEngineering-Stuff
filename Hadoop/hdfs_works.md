NOTE: Don't forget to start services.
    docker exec -it cluster-master "/usr/local/hadoop/spark-services.sh"

### Q-1: 
- Download and put `https://raw.githubusercontent.com/erkansirin78/datasets/master/Wine.csv` dataset in hdfs `/user/train/hdfs_odev` directory.
first download the dataset in your project file:

wget url...

hdfs dfs -mkdir -p /user/train/hdfs_odev
Checking: 
    hdfs dfs -ls /
hdfs dfs -copyFromLocal Wine.csv /user/train/hdfs_odev/
checking:
    hdfs dfs -ls /user/train/hdfs_odev/

### Q-2:
- Copy this hdfs file `/user/train/hdfs_odev/Wine.csv` to `/tmp/hdfs_odev` hdfs directory.

hdfs dfs -mkdir -p /tmp/hdfs_odev/
hdfs dfs -cp /user/train/hdfs_odev/Wine.csv /tmp/hdfs_odev/
hdfs dfs -ls /tmp/hdfs_odev/
check the Wine.csv file:
    hdfs dfs -ls /tmp/hdfs_odev/

### Q-3:
- Delete `/tmp/hdfs_odev` directory with skipping the trash. 

hdfs dfs -rm -r -skipTrash /tmp/hdfs_odev
Checking:
    hdfs dfs -ls /tmp/hdfs_odev/

### Q-4:
-  Explore `/user/train/hdfs_odev/Wine.csv` file from web hdfs.  

hadoop panel: 
    127.0.0.1:8088
Cluster-master-node panel:
    127.0.0.1:50070
    utilities > browse the file system > user > train > hdfs_odev > Wine.csv 