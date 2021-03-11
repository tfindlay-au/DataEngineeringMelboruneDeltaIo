# Apache Spark with Delta-io 
Shared at Data Engineering Melbourne on 4th March 2021

## Overview
This project demonstrates how you can use a simple Scala application with Apache Spark and Delta-io to efficiently merge data.

The code used in this project is based on the free open source versions of:
- Scala
- Apache Spark
- Delta-io

Caveats:
This is not sponsored by Databricks and views expressreed are entirely my own.

## What you need
For this project you will need:
- Computer (OSX/Windows/Linux)
- Java Virtual Machine
- Internet connection to download dependencies from ivy

Using the right version can help with compatibility, using other versions may yield different results

|Tool | Version|
|-----|--------|
|Scala|2.12.x  |
|Spark|3.0.0   |
|Delta|0.8.0   |


## Project structure
In the `src/main/scala` folder you will find:
- HelloWorld.scala: This is a minimal program to check we can compile and run the program.
- HelloWorldWithSpark.scala: This adds the Spark dependency and tests its ability to create a small dataframe and display the output
- HelloWorldWithSparkDelta.scala: This adds delta framework and confiures Spark to use it

Two additional scripts added to help demonstrate performance at scale
- DataGen.scala: Simple crude generate to attempt to make some data at scale
- DataMerge.scala: Simple script to generate a sequential update to be merged into datagen

## Getting Started

### Building
Use the Simple Build Tool(sbt) found as [https://www.scala-sbt.org/](https://www.scala-sbt.org/)
```aidl
sbt build
```

### Running
```
sbt run
or
java -cp .\target\scala-2.12\classes;\path\to\scala-library-2.12.13.jar HelloWorld
```

## Possible issues
1. On a windows platform, you may need to download a hadoop distribution for Spark to read/write.
   
   HADOOP_HOME=C:\Users\MyName\Somepath\winutils\hadoop-2.6.4\

2. Also on windows, be sure to use the correct backslash for paths and semi-colon for the java classpath
