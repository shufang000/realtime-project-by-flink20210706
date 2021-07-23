#!/bin/bash
JAVACMD=$JAVA_HOME/bin/java
APP_NAME=logger-process-0.0.1.jar
case $1 in 
	"start")
	{
		for i in shufang101	shufang102	shufang103
		do 
			echo ">>>>>>>>>>>>> $i starting processing jar ....<<<<<<<<<<<<<<<"
			ssh $i "$JAVACMD -Xms32m -Xmx64m -jar /opt/module/realtime_20210703/logger_process/$APP_NAME  >/dev/null 2?&1 &"
		done 
		echo ">>>>>>>>>>>>>processing jar was started on $i<<<<<<<<<<<<<<<"
		
		echo ">>>>>>>>>>>>> $i starting nginx ....<<<<<<<<<<<<<<<"
		/opt/module/nginx/sbin/nginx 
		echo ">>>>>>>>>>>>> nginx was started on $i ....<<<<<<<<<<<<<<<"
	};;
	"stop")
	{
		# 首先停止nginx
		echo ">>>>>>>>>>>>> $i stoping nginx ....<<<<<<<<<<<<<<<"
		/opt/module/nginx/sbin/nginx -s stop
		echo ">>>>>>>>>>>>> nginx was stoped on $i ....<<<<<<<<<<<<<<<"
		for i in shufang101	shufang102	shufang103
		do 
			echo ">>>>>>>>>>>>> $i stoping processing jar ....<<<<<<<<<<<<<<<"
			ssh $i "source /etc/profile;jps | grep '$APP_NAME' | grep -v grep | awk '{print \$1}' | xargs kill"
		done 
	};;
esac 
