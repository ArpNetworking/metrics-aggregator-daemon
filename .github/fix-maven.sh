#!/bin/bash
MAVEN_HOME=$(./jdk-wrapper.sh ./mvnw -v | awk '/^Maven home:/ { print $3 }')
if [ -d "$MAVEN_HOME" ]; then
  echo "Installing custom wagon-http jar to $MAVEN_HOME/lib"
  ls -l "$MAVEN_HOME/lib/"wagon-http-*-shaded.jar
  rm "$MAVEN_HOME/lib/"wagon-http-*-shaded.jar
  cp ./.github/wagon-http-* "$MAVEN_HOME/lib/"
  ls -l "$MAVEN_HOME/lib/"wagon-http-*-shaded.jar
else
  echo "Unable to fix Maven!"
fi
