//
// Author - Jack Hebert (jhebert@cs.washington.edu)
// Copyright 2007
// Distributed under GPLv3
//
package Pagerank.PageRankIter;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.Text;

public class PageRankIterReducer extends MapReduceBase implements Reducer {
	  private static void debug(String s) {
		  System.out.println(s);
	  }
  public void reduce(WritableComparable key, Iterator values,
                     OutputCollector output, Reporter reporter) throws IOException {
    double score = 0;
    String outLinks = "";
//    final double n = 6552490;
    final double n = 4;
    final double damping = 0.15;
    debug(key.toString());
    while (values.hasNext()) {
      String curr = ((Text)values.next()).toString();
      int colon = curr.indexOf(":");
      if ((colon > -1)) {
        try {
          outLinks = curr.substring(colon + 1);
          continue;
        } catch (Exception e) {
          ;
        }
      } else {
        score += Double.parseDouble(curr);
      }
    }
    score = damping/n + (1.-damping)*score;
    String toEmit;
    if (outLinks.length() > 0) {
      toEmit = (new Double(score)).toString() + ":" + outLinks;
    } else {
      toEmit = (new Double(score)).toString();
    }
    
    output.collect(key, new Text(toEmit));
  }
}
