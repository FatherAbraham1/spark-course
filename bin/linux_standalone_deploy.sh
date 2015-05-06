# 1 安装jdk
# http://www.oracle.com/technetwork/java/javase/downloads/index.html
# /etc/profile
#export JAVA_HOME=/usr/share/jdk1.6.0_14 
#export PATH=$JAVA_HOME/bin:$PATH 
#export CLASSPATH=.:$JAVA_HOME/lib/dt.jar:$JAVA_HOME/lib/tools.jar

# 2 下载Spark bin
# http://spark.apache.org/downloads.html

# 3 tar -xzvf

# 4 配置conf/spark-env.sh
# export SPARK_MASTER_IP=

# 5 配置conf/slaves

# 6 下载pssh
# https://github.com/knktc/insecure_pssh

# 7 拷贝Spark到其他节点

# 8 sbin/start-all.sh

# 9 运行示例
# ./run-example org.apache.spark.examples.SparkPi spark://masterIP:7077
./bin/spark-submit \
  --class org.apache.spark.examples.SparkPi \
  --master spark://masterIP:7077 \
  --executor-memory 20G \
  --total-executor-cores 100 \
  /mnt/diskd/yanjie/spark-1.2.1-bin-hadoop2.4/lib/spark-examples-1.2.1-hadoop2.4.0.jar \
  1000
