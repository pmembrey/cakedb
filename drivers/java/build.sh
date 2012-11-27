#compile class files
javac -cp /usr/share/java/junit4.jar org/cakedb/drivers/java/Event.java org/cakedb/drivers/java/Driver.java org/cakedb/drivers/java/UnitTestDriver.java
#run unit test
java -cp .:/usr/share/java/junit4.jar org.junit.runner.JUnitCore org.cakedb.drivers.java.UnitTestDriver
#build jar
jar cf cakedb001.jar org/cakedb/drivers/java/Event.class org/cakedb/drivers/java/Driver.class
