YCSB_ARGS=
YCSB_ARGS="${YCSB_ARGS} -P workloads/workloada"
YCSB_ARGS="${YCSB_ARGS} -db com.ibm.research.mongotx.ycsb.MongoTxDriver"
YCSB_ARGS="${YCSB_ARGS} -s"
YCSB_ARGS="${YCSB_ARGS} -threads 8"
YCSB_ARGS="${YCSB_ARGS} -p recordcount=1000"
YCSB_ARGS="${YCSB_ARGS} -p operationcount=100000"
YCSB_ARGS="${YCSB_ARGS} -p mongodb.url=mongodb://localhost:27017/ycsb"


CP=
CP=${CP}:../target/classes
CP=${CP}:../target/test-classes
CP=${CP}:../lib/core-0.1.4.jar

CP=${CP}:${MAVEN_DIR}/org/mongodb/mongo-java-driver/3.0.4/mongo-java-driver-3.0.4.jar
CP=${CP}:${MAVEN_DIR}/junit/junit/4.11/junit-4.11.jar
CP=${CP}:${MAVEN_DIR}/org/hamcrest/hamcrest-core/1.3/hamcrest-core-1.3.jar
CP=${CP}:${MAVEN_DIR}/javax/servlet/servlet-api/2.4/servlet-api-2.4.jar
CP=${CP}:${MAVEN_DIR}/javax/servlet/javax.servlet-api/3.0.1/javax.servlet-api-3.0.1.jar
CP=${CP}:${MAVEN_DIR}/javax/ejb/ejb-api/3.0/ejb-api-3.0.jar
CP=${CP}:${MAVEN_DIR}/commons-logging/commons-logging/1.2/commons-logging-1.2.jar

CLASS=com.ibm.research.mongotx.ycsb.YCSB