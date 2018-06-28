FROM bde2020/spark-base:2.3.0-hadoop2.7

ENV HADOOP_CONF_DIR=/etc/hadoop/

ENV YARN_CONF_DIR=/etc/hadoop/

COPY hdfs-site.xml /etc/hadoop/

COPY core-site.xml /etc/hadoop/

COPY yarn-site.xml /etc/hadoop/

COPY target/scala-2.11/spark-demo_2.11-1.0.0.jar sparkdemo.jar

ENTRYPOINT ["/spark/bin/spark-submit"]

CMD ["--master", "yarn", "--deploy-mode", "cluster", "--executor-memory", "1G", "--num-executors", "2", "--conf", "dfs.client.use.datanode.hostname=true", "--class", "com.sparkdemo.Start", "sparkdemo.jar"]
