#!/bin/bash

cd src/main

#Delete intermediate and output directories if they exist
rm -r intermediate
rm -r output

#Compile all java classes
find . -name "*.java" -print | xargs javac

#Run applications (tests) and delete intermediate files everytime
java applications/applicationLogs/ApplicationLogs.java "FALSE"
rm -r intermediate
java applications/mutualFriends/MutualFriends.java "FALSE"
rm -r intermediate
java applications/wordCount/WordCount.java "FALSE"
rm -r intermediate
java applications/serverRequest/ServerRequest.java "FALSE"
cmd /k