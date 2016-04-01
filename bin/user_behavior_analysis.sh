#!/bin/bash
# Script to start the job server
# Extra arguments will be spark-submit options, for example
#  ./server_start.sh --jars cassandra-spark-connector.jar
#
# Environment vars (note settings.sh overrides):
#   JOBSERVER_MEMORY - defaults to 1G, the amount of memory (eg 512m, 2G) to give to job server
#   JOBSERVER_CONFIG - alternate configuration file to use
#   JOBSERVER_FG    - launches job server in foreground; defaults to forking in background
set -e

get_abs_script_path() {
  pushd . >/dev/null
  cd "$(dirname "$0")"
  appdir=$(pwd)
  popd  >/dev/null
}

get_abs_script_path

#. $appdir/setenv.sh


# To truly enable JMX in AWS and other containerized environments, also need to set
# -Djava.rmi.server.hostname equal to the hostname in that environment.  This is specific
# depending on AWS vs GCE etc.
JAVA_OPTS="-XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled -XX:MaxDirectMemorySize=512M -Dderby.stream.error.file=/dev/null -Djava.net.preferIPv4Stack=true"
LOGGING_OPTS="-Dlog4j.configuration=file:$appdir/log4j.properties -DLOG_DIR=$appdir/logs/user-behavior/"
MAIN="com.laanto.it.userbehavior.MicroShopUserBehaviorJob"
NAME="microShop-user-behavior"
MASTER="spark://node2.laanto.com:7077"
DEPLOY_MODE="client"
DRIVER_MEMORY="1g"
EXECUTOR_MEMORY="2g"
EXECUTOR_CORES="3"
TOTAL_EXECUTOR_CORES="9"
JAR="user-behavior-analysis-1.0.0.jar"

cmd='$SPARK_HOME/bin/spark-submit --master $MASTER --deploy-mode $DEPLOY_MODE --name $NAME 
  --class $MAIN --driver-memory $DRIVER_MEMORY --executor-memory $EXECUTOR_MEMORY
  --executor-cores $EXECUTOR_CORES --total-executor-cores $TOTAL_EXECUTOR_CORES  
  --driver-java-options "$JAVA_OPTS $LOGGING_OPTS"
  $appdir/$JAR $@'

#eval $cmd > /dev/null 2>&1 
eval $cmd 
