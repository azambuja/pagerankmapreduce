package Pagerank.PageRankIter;

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.Text;


public class PageRankIterMapper extends MapReduceBase implements Mapper {

	  private static void debug(String s) {
		  System.out.println(s);
	  }
	
  public void map(WritableComparable key, Writable value,
                  OutputCollector output, Reporter reporter) throws IOException {
	
    String data = ((Text)value).toString();
    String data2 = data;
    int index = data.indexOf(":");
    if (index == -1) {
      return;
    }
    String toParse = data.substring(0, index).trim();
    double currScore = Double.parseDouble(toParse);
    debug("data antes: " + key.toString() + " " + data);
    data = data.substring(index+1);
    debug("data: " + key.toString() + " " + data);
    String[] pages = data.split("_");
    Text toEmit = new Text((new Double(currScore / pages.length)).toString());
    for (String page : pages) {
      debug(page +" " + toEmit.toString());
      output.collect(new Text(page.trim()), toEmit);
    }
    debug("Output = " + data.toString());
    output.collect(key, new Text(data2));
  }
}
