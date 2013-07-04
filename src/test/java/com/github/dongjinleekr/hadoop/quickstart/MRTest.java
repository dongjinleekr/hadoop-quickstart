package com.github.dongjinleekr.hadoop.quickstart;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class MRTest {
  private MapDriver<Text, Text, Text, IntWritable> mapDriver;
  private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
  private MapReduceDriver<Text, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

  @Before
  public void setUp() throws Exception {
    WordCountMapper mapper = new WordCountMapper();
    WordCountReducer reducer = new WordCountReducer();

    mapDriver = MapDriver.newMapDriver(mapper);
    reduceDriver = ReduceDriver.newReduceDriver(reducer);
    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
  }

  @Test
  public void testMap() throws IOException {
    mapDriver.withInput(new Text(), new Text("korea"))
      .withInput(new Text(), new Text("seoul"));
    mapDriver.withOutput(new Text("korea"), new IntWritable(1))
      .withOutput(new Text("seoul"), new IntWritable(1));
    mapDriver.runTest();
  }

  @Test
  public void testReduce() throws IOException {
    List<IntWritable> values = new ArrayList<IntWritable>();
    values.add(new IntWritable(3));
    values.add(new IntWritable(2));
    reduceDriver.withInput(new Text("seoul"), values);
    reduceDriver.withOutput(new Text("seoul"), new IntWritable(5));
    reduceDriver.runTest();
  }

  public void testMapReduce() throws IOException {
    mapReduceDriver.withInput(new Text(), new Text("seoul"))
        .withInput(new Text(), new Text("austin"))
        .withInput(new Text(), new Text("tokyo"))
        .withInput(new Text(), new Text("austin"))
        .withInput(new Text(), new Text("tokyo"))
        .withInput(new Text(), new Text("tokyo"))
        .withOutput(new Text("seoul"), new IntWritable(1))
        .withOutput(new Text("austin"), new IntWritable(2))
        .withOutput(new Text("tokyo"), new IntWritable(3)).runTest();
  }
}
