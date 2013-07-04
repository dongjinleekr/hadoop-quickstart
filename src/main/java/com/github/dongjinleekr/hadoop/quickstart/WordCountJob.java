package com.github.dongjinleekr.hadoop.quickstart;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.math.VectorWritable;

import com.google.common.base.Stopwatch;

public class WordCountJob extends AbstractJob {
  private static final Log logger = LogFactory.getLog(WordCountJob.class); 

  public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
    addInputOption();
    addOutputOption();
    addOption(DefaultOptionCreator.dummyOption().create());
    
    if (parseArguments(args) == null) {
      return -1;
    }
    
    if (null == getConf()) {
      setConf(new Configuration());
    }
    
    Path input = getInputPath();
    Path output = getOutputPath();
    int dummy = Integer.parseInt(getOption(DefaultOptionCreator.DUMMY));
     
    return run(getConf(), input, output, dummy);
  }
  
  public int run(Configuration conf, Path input, Path output, int dummy) throws IOException, ClassNotFoundException, InterruptedException {
    conf.setInt(DefaultOptionCreator.DUMMY, dummy);

    // current datetime
    DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    Date current = new Date();

    // task name
    String jobName = String.format("hadoop-quickstart (%s)", dateFormat.format(current));
    logger.info(String.format("Job Name: %s", jobName));

    // thread count
    Runtime runtime = Runtime.getRuntime();
    int processorCount = runtime.availableProcessors();
    logger.info(String.format("available cores: %d", processorCount));
    int threadCount = 2 * processorCount;
    logger.info(String.format("available threads: %d", threadCount));

    // create Job
    Job job = new Job(conf);

    job.setJarByClass(getClass());
    job.setJobName(jobName);
    
    // input
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setMapperClass(WordCountMapper.class);
    
    // Note: If you want to run a multithreaded mapper, change above line to following:
    // job.setMapperClass(MultithreadedMapper.class);
    // MultithreadedMapper.setMapperClass(job, Map.class);
    // MultithreadedMapper.setNumberOfThreads(job, threadCount);
    FileInputFormat.addInputPath(job, input);

    // intermediate
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    // output
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setReducerClass(WordCountReducer.class);
    FileOutputFormat.setOutputPath(job, output);

    job.setNumReduceTasks(1);

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static int main(String[] args) throws Exception {
    // execute Job
    Stopwatch stopwatch = new Stopwatch().start();
    int ret = ToolRunner.run(new WordCountJob(), args);
    stopwatch.stop();

    logger.info(String.format("Time Elapsed: %d (ms)", stopwatch.elapsed(TimeUnit.MILLISECONDS)));

    return ret;
  }
}
