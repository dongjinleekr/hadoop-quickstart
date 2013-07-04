# hadoop-quickstart

An Bootstrap implementation for [apache hadoop](http://hadoop.apache.org/) or [apache mahout](http://mahout.apache.org/) projects.

## How to install mrunit

git clone git://github.com/apache/mrunit.git
git checkout [tag-name] # for example, 'release-1.0.0-hadoop1'
mvn install

## How to run

hadoop fs -copyFromLocal testtxt.seq /testtxt.seq
mvn compile
mvn exec:java -Dexec.mainClass="com.github.dongjinleekr.hadoop.quickstart.WordCountJob" -Dexec.args="-fs hdfs://localhost:54310 -e 10 -i /testtxt.seq -o /counted.out"
mahout seqdumper -i /counted.out/part-r-00000

![hadoop logo](http://hadoop.apache.org/images/hadoop-logo.jpg)

![mahout logo](http://mahout.apache.org/images/mahout-logo.png)

## License

Copyright (C) 2013 [Dongjin Lee](http://blog.dongjinleekr.com). <dongjin.lee.kr@gmail.com>

Licensed under the Apache License, Version 2.0