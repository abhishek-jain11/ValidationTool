@echo off
rem To run this batch file, please pass arguments like below
jar uf lib\hana-migrator.jar ValidationTests.properties
java -Dlog4j.configurationFile="log4j2.xml" -jar lib\hana-migrator.jar %*
