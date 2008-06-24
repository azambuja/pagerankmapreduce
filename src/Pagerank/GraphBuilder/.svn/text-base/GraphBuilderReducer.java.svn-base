//
// Author - Jack Hebert (jhebert@cs.washington.edu)
// Copyright 2007
// Distributed under GPLv3
//
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import java.lang.StringBuilder;

public class GraphBuilderReducer extends MapReduceBase implements Reducer {

  public void reduce(WritableComparable key, Iterator values,
                     OutputCollector output, Reporter reporter) throws IOException {

    reporter.setStatus(key.toString());
    String toWrite = "";
    int count = 0;
    while (values.hasNext()) {
      String page = ((Text)values.next()).toString();
      page.replaceAll(" ", "_");
      toWrite += " " + page;
      count += 1;
    }

    String num = (new Integer(count)).toString();
    toWrite = num + ":" + toWrite;
    output.collect(key, new Text(toWrite));
  }
}
