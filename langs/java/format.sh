#!/bin/bash
java -jar external/google-java-format-1.7-all-deps.jar -i `find java | grep "\.java$"`
