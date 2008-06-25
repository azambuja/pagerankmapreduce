package Pagerank.GraphBuilder;

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
	  private static void debug(String s) {
		  System.out.println(s);
	  }
  public void reduce(WritableComparable key, Iterator values,
                     OutputCollector output, Reporter reporter) throws IOException {
//	final double n = 6552490;
	final double n = 4;
	debug(key.toString());
	reporter.setStatus(key.toString());
    String toWrite = "";
    
    while (values.hasNext()) {
      String page = ((Text)values.next()).toString();
      page = page.trim().replaceAll(" ", "_");
      debug("   page = " + page);
      toWrite += " " + page;
      debug("   toWrite = " + toWrite);
    }

    String num = (new Double(1./n)).toString();
    debug("   toWrite final antes = " + toWrite);
    toWrite = num + ": " + toWrite;
    debug("   toWrite final = " + toWrite);
    output.collect(key, new Text(toWrite));
  }
}
