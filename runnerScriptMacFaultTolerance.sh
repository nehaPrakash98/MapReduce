#!/bin/bash


cd src/main

#Delete intermediate and output directories if they exist
rm -r intermediate
rm -r output

#Compile all java classes
find . -name "*.java" -print | xargs javac

#Run word count application with induce Fault param set to TRUE
java applications/wordCount/WordCount.java "TRUE"