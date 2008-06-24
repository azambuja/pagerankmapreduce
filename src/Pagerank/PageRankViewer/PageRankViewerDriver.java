//
// Author - Jack Hebert (jhebert@cs.washington.edu)
// Copyright 2007
// Distributed under GPLv3
//
package Pagerank.PageRankViewer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;


public class PageRankViewerDriver {

  public static void main(String[] args) {
    JobClient client = new JobClient();
    JobConf conf = new JobConf(PageRankViewerDriver.class);
    conf.setJobName("Page-rank Viewer");

    conf.setInputFormat(org.apache.hadoop.mapred.SequenceFileInputFormat.class);
    conf.setOutputKeyClass(FloatWritable.class);
    conf.setOutputValueClass(Text.class);

/*
    if (args.length < 2) {
      System.out.println("Usage: PageRankIter <input path> <output path>");
      System.exit(0);
    }
 */

//    conf.setInputPath(new Path(args[0]));
//    conf.setOutputPath(new Path(args[1]));
    conf.setInputPath(new Path("out"));
    conf.setOutputPath(new Path("out2"));

    
    conf.setMapperClass(PageRankViewerMapper.class);
    conf.setReducerClass(org.apache.hadoop.mapred.lib.IdentityReducer.class);

    client.setConf(conf);
    try {
      JobClient.runJob(conf);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
