###How to build
1. Download Daytrader3Sample.jar from [IBM site](https://developer.ibm.com/wasdev/downloads/#asset/samples-Daytrader3_Sample) and store it into /samples/daytrader/ and deploy it in the installed WebSphere Liberty.
2. Add /samples/daytrader/src as your eclipse build path of mongo-tx (and build code in the directory).
3. `ant ear`. ant will generate dist/daytrader3-ee6.ear.

###How to deploy
1. Download WebSphere Liberty runtime from [IBM site](https://developer.ibm.com/wasdev/downloads/liberty-profile-using-non-eclipse-environments/) and install it in your machine (`$LIBERTY_HOME`).
2. `cd $LIBERTY_HOME` and `unzip Daytrader3Sample.jar`.
3. Copy server.xml to `$LIBERTY_HOME/usr/servers/Daytrader3Sample/`
4. Copy mongo-java-driver-3.0.4.jar commons-logging-1.2.jar in `$LIBERTY_HOME/lib`.
5. `mkdir $LIBERTY_HOME/usr/servers/Daytrader3Sample/apps/` and `rm $LIBERTY_HOME/usr/servers/Daytrader3Sample/dropins/*`.
6. Copy dist/daytrader3-ee6.ear to `$LIBERTY_HOME/usr/servers/Daytrader3Sample/apps/`.
7. Start your MongoDB with 27017 TCP/IP port.
8. Start Liberty (`bin/server start Daytrader3Sample`).
9. Enjoy trading via http://localhost:9080/daytrader/ or http://localhost:9080/daytrader/scenario . 

