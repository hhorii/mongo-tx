###How to build
1. Import the mongo-tx directory (the top directory of this repository) with Eclipse as a Maven project (Right click in "Package Explorer"->Import->"Existing Maven Projects"->select the mongo-tx directory as the root->select pom.xml in the directory).
2. Download Daytrader3Sample.jar from [IBM site](https://developer.ibm.com/wasdev/downloads/#asset/samples-Daytrader3_Sample) and store it into /samples/daytrader/.
3. open /samples/daytrader/build.xml and run "extract" task, then refresh the mongo-tx project (with F5 ususally). You can find some files and directories are extracted under /samples/daytrader/.
4. Add /samples/daytrader/Daytrader3Sample/dt-ejb.jar into the build path and add /samples/daytrader/src/java as an eclipse source directory for the mongo-tx project (and build code in the directory).
5. Confirm no error exists in mongo-tx project.
6. Run `ant ear` with /samples/daytrader/build.xml. ant will generate /samples/daytrader/dist/daytrader3-ee6.ear.

###How to deploy
1. Download WebSphere Liberty runtime from [IBM site](https://developer.ibm.com/wasdev/downloads/liberty-profile-using-non-eclipse-environments/) and install it in your machine (`$LIBERTY_HOME`).
2. `cd $LIBERTY_HOME` and `unzip Daytrader3Sample.jar`.
3. Copy /samples/daytrader/server.xml to `$LIBERTY_HOME/usr/servers/Daytrader3Sample/`
4. Copy mongo-java-driver-3.0.4.jar commons-logging-1.2.jar (you will be find ) in `$LIBERTY_HOME/Daytrader3Sample/lib`.
5. `rm $LIBERTY_HOME/usr/servers/Daytrader3Sample/dropins/*`.
6. `mkdir $LIBERTY_HOME/usr/servers/Daytrader3Sample/apps/`
7. Copy dist/daytrader3-ee6.ear to `$LIBERTY_HOME/usr/servers/Daytrader3Sample/apps/`.
8. Start your MongoDB with 27017 TCP/IP port.
9. Start Liberty (`bin/server start Daytrader3Sample`).
10. Enjoy trading via http://localhost:9080/daytrader/ or http://localhost:9080/daytrader/scenario . 

