//
// Author - Jack Hebert (jhebert@cs.washington.edu)
// Copyright 2007
// Distributed under GPLv3
//
package Pagerank.PageRankViewer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.*;

public class PageRankViewerMapper extends MapReduceBase implements Mapper {

   private static void debug(String s) {
	  System.out.println(s);
   }
	  
  public void map(WritableComparable key, Writable value,
                  OutputCollector output, Reporter reporter) throws IOException {

    String data = ((Text)value).toString();
    int index = data.indexOf(":");
    if (index == -1) {
      return;
    }
    String toParse = data.substring(0, index).trim();
    double currScore = Double.parseDouble(toParse);
    
    debug((new Double(currScore)).toString() + " " + key);
    
    output.collect(new FloatWritable((float) - currScore), key);
    //output.collect((WritableComparable) value, key);
  }
}
