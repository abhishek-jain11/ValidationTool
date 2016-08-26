#!/bin/bash
#jar uf lib/hana-migrator.jar ValidationTests.properties
java -Dlog4j.configurationFile="log4j2.xml" -jar lib/hana-migrator.jar "$@"
