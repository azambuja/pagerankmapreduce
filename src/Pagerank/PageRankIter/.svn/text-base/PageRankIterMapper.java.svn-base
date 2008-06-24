//
// Author - Jack Hebert (jhebert@cs.washington.edu)
// Copyright 2007
// Distributed under GPLv3
//
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.Text;


public class PageRankIterMapper extends MapReduceBase implements Mapper {

  public void map(WritableComparable key, Writable value,
                  OutputCollector output, Reporter reporter) throws IOException {

    String data = ((Text)value).toString();
    int index = data.indexOf(":");
    if (index == -1) {
      return;
    }
    String toParse = data.substring(0, index).trim();
    double currScore = Double.parseDouble(toParse);
    data = data.substring(index+1);
    String[] pages = data.split(" ");
    Text toEmit = new Text((new Double(.98 * currScore / pages.length)).toString());
    for (String page : pages) {
      output.collect(new Text(page), toEmit);
    }
    output.collect(key, new Text(".02"));
    output.collect(key, new Text(" " + data));
  }
}
