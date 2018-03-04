DT3_ARGS=
DT3_ARGS="$DT3_ARGS -Dusers=50000 -Dquotes=10000"
DT3_ARGS="$DT3_ARGS -Dclient=20"
DT3_ARGS="$DT3_ARGS -Drampup=30"
DT3_ARGS="$DT3_ARGS -Dmeasure=300"

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

LOAD_CLASS=com.ibm.research.mongotx.dt3.DT3Load
CLASS=com.ibm.research.mongotx.dt3.DT3
