package com.github.dongjinleekr.hadoop.quickstart;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.common.base.Splitter;

public class WordCountMapper extends Mapper<Text, Text, Text, IntWritable> {
  private static final Log logger = LogFactory.getLog(WordCountMapper.class);

  private final Set<String> stopwords = new HashSet<String>();
  private final Splitter splitter = Splitter.on('\n').omitEmptyStrings().trimResults();
  private final static IntWritable one = new IntWritable(1);
  private Text word = new Text();

  @Override
  protected void setup(Context context) throws IOException {
    Configuration conf = context.getConfiguration();
    FileSystem fs = FileSystem.get(conf);
    URI[] cacheUris = DistributedCache.getCacheArchives(conf);

    if (null == cacheUris) {
      logger.info(String.format("DistributedCache not configured."));
    } else {
      logger.info(String.format(String.format("DistributedCache configured: %d files.",
          cacheUris.length)));

      for (URI cacheUri : cacheUris) {
        Path cachePath = new Path(cacheUri);

        try (SequenceFile.Reader reader = new SequenceFile.Reader(fs, cachePath, conf)) {
          IntWritable key = new IntWritable();
          Text value = new Text();

          while (reader.next(key, value)) {
            int k = key.get();
            String str = value.toString();

            stopwords.add(str);
            logger.info(String.format("stopword added: ", str));
          }
        }
      }
    }
  }

  @Override
  public void map(Text key, Text value, Context context) throws IOException,
      InterruptedException {
    String line = value.toString();
    for (String str : splitter.split(line)) {
      word.set(str);
      context.write(word, one);
    }
  }
}
