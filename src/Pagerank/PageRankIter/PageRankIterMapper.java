package Pagerank.PageRankIter;

import Pagerank.PageRankIter.PageRankIterDriver;

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.io.Text;


public class PageRankIterMapper extends MapReduceBase implements Mapper {

	  private static void debug(String s) {
		  System.out.println(s);
	  }
	
  public void map(WritableComparable key, Writable value,
                  OutputCollector output, Reporter reporter) throws IOException {
    Counters counters = PageRankIterDriver.counters;
    String data = ((Text)value).toString();
    debug("data = " + data);
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
    data = data.trim();
    if (data.length() != 0) {
      String[] pages = data.split(" ");
      Text toEmit = new Text((new Double(currScore / pages.length)).toString());
      for (String page : pages) {
        debug(key.toString() + " deu contribuicao para " + page +" " + toEmit.toString());
        output.collect(new Text(page.trim()), toEmit);
      }
      debug("Output = " + data.toString());
      output.collect(key, new Text(data2));
    } else {
        counters.incrCounter(PageRankIterDriver.Link.LinksToNoOne, (new Double(1.e12*currScore)).longValue());
    }
  }
}
