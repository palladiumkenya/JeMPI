ARG JAVA_VERSION

FROM eclipse-temurin:${JAVA_VERSION}-jre

ADD AsyncReceiver-1.0-SNAPSHOT-spring-boot.jar /app/AsyncReceiver-1.0-SNAPSHOT-spring-boot.jar

RUN printf "#!/bin/bash\n\
  cd /app\n\
  java -server -XX:MaxRAMPercentage=80 -jar /app/AsyncReceiver-1.0-SNAPSHOT-spring-boot.jar\n" > /entrypoint.sh

RUN chmod +x /entrypoint.sh
RUN mkdir /app/csv
RUN mkdir /app/sql

ENTRYPOINT ["/entrypoint.sh"] 
