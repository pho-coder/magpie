#!/usr/bin/env bash
bin=`dirname $0`
bin=`cd "$bin"; pwd`
MAGPIE_PROJECT=magpie-checkpoint
DEFAULT_CONF_DIR="$bin"/../conf
DEFAULT_HOME=`cd "$bin"/..;pwd`
MAGPIE_CONF_DIR=${MAGPIE_CONF_DIR:-$DEFAULT_CONF_DIR}
MAGPIE_CHECKPOINT_HOME=${MAGPIE_CHECKPOINT_HOME:-$DEFAULT_HOME}
MAGPIE_LOG_DIR=${MAGPIE_LOG_DIR:-$MAGPIE_CHECKPOINT_HOME/logs}
MAGPIE_NATIVE_DIR=${MAGPIE_NATIVE_DIR:-$MAGPIE_CHECKPOINT_HOME/lib/native/Linux-amd64-64}
PID_FILE="$MAGPIE_LOG_DIR/.${MAGPIE_PROJECT}.run.pid"
mkdir -p $MAGPIE_LOG_DIR
MAGPIE_HEAP_OPTS="-Xmx256M -Xms128M"

function running(){
	if [ -f "$PID_FILE" ]; then
		pid=$(cat "$PID_FILE")
		process=`ps aux | grep " $pid " | grep -v grep`;
		if [ "$process" == "" ]; then
	    		return 1;
		else
			return 0;
		fi
	else
		return 1
	fi	
}

function start_server() {
	if running; then
		echo "is running."
		exit 1
	fi
        readonly MASTER_JARS="$(ls "$MAGPIE_CHECKPOINT_HOME"/magpie-queue-*.jar "$MAGPIE_CHECKPOINT_HOME"/target/magpie-queue-*.jar 2> /dev/null | grep -v magpie-client-*.jar | tr "\n" :)"
        if [ -n "${JAVA_HOME}" ]; then
    	  RUNNER="${JAVA_HOME}/bin/java"
	else
    	  if [ `command -v java` ]; then
            RUNNER="java"
    	  else
            echo "JAVA_HOME is not set" >&2
            exit 1
     	  fi
        fi
	CLASSPATH="$MAGPIE_CONF_DIR:$MASTER_JARS"
	nohup "$RUNNER" $MAGPIE_HEAP_OPTS -cp "$CLASSPATH" -Djava.library.path="$MAGPIE_NATIVE_DIR" -Dmagpie.configuration="$MAGPIE_CONF_DIR" -Dlog4j.configuration=log4j.properties -Dmagpie.home="$MAGPIE_CHECKPOINT_HOME" -Dmagpie.project="$MAGPIE_PROJECT" -Dzookeeper.servers=BJHC-HBase-Magpie-17896.jd.local:2181,BJHC-HBase-Magpie-17895.jd.local:2181,BJHC-HBase-Magpie-17894.jd.local:2181,BJHC-HBase-Magpie-17893.jd.local:2181,BJHC-HBase-Magpie-17897.jd.local:2181 -Dzookeeper.root=/hbase_magpie -Dcheckpoint.tablename=checkpoint_record com.jd.bdp.magpie.queue.MainExecutor > /dev/null 2>&1 &
}

function stop_server() {
	if ! running; then
		echo "${MAGPIE_PROJECT} is not running."
		#exit 1
	fi
	count=0
	pid=$(cat $PID_FILE)
	while running;
	do
	  let count=$count+1
	  echo "Stopping $count times"
	  if [ $count -gt 5 ]; then
	      echo "kill -9 $pid"
	      kill -9 $pid
	  else
	      kill $pid
	  fi
	  sleep 3;
	done	
	echo "Stop ${MAGPIE_PROJECT} successfully." 
	rm $PID_FILE
}

function status(){
        if running;then
		echo "${MAGPIE_PROJECT} is running."
        else
                echo "${MAGPIE_PROJECT} is not running."
        fi
        exit 0
}

function help() {
    echo "Usage: magpie-checkpoint-daemon.sh {start|stop}" >&2
    echo "       start:             start"
    echo "       stop:              stop"
}

command=$1
shift 1
case $command in
    status)
        status $@;
        ;;        
    start)
        start_server $@;
        ;;    
    stop)
        stop_server $@;
        ;;
    restart)
        stop_server $@;
        start_server $@;
        ;;
    *)
        help;
        exit 1;
        ;;
esac
