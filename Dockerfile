FROM hseeberger/scala-sbt:8u141_2.12.4_1.0.2

ADD build.sbt /rdb-connector-mysql/build.sbt
ADD project /rdb-connector-mysql/project
ADD src /rdb-connector-mysql/src
ADD run_it_tests.sh /rdb-connector-mysql/run_it_tests.sh

WORKDIR /rdb-connector-mysql

RUN apt-get update && apt-get install -y mysql-client

RUN sbt clean compile
