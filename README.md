# hadoop-quickstart

An Bootstrap implementation for [apache hadoop](http://hadoop.apache.org/) or [apache mahout](http://mahout.apache.org/) projects. You can easily start your own hadoop project with this, without configuration.

![hadoop logo](http://hadoop.apache.org/images/hadoop-logo.jpg)

![mahout logo](http://mahout.apache.org/images/mahout-logo.png)

## Prerequsitives
* JDK (>= 7)
* maven (>= 3.0)
* hadoop (>= 0.23.203)

## How to install mrunit
To run unit tests on MapReduce code, you must install mrunit on your local maven repository first. To install it, execute following commands:

> git clone git://github.com/apache/mrunit.git
> 
> git checkout [tag-name] # for example, 'release-1.0.0-hadoop1'
> 
> mvn install

## How to build

> mvn clean package

## How to run

> hadoop fs -copyFromLocal testtxt.seq /testtxt.seq # copy included file onto your hdfs
> 
> hadoop jar target/hadoop-quickstart-1.0-job.jar com.github.dongjinleekr.hadoop.quickstart.WordCountJob -e 10 -i /testtxt.seq -o /counted.out

You can check the output with following command (requires [mahout](http://mahout.apache.org/)):

> mahout seqdumper -i /counted.out/part-r-00000

## License

Copyright (C) 2013 [Dongjin Lee](http://blog.dongjinleekr.com). <dongjin.lee.kr@gmail.com>

Licensed under the Apache License, Version 2.0