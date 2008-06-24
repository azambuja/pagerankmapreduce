package Pagerank.GraphBuilder;

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import org.apache.hadoop.io.Text;
import java.util.*;
import java.lang.StringBuilder;

/*
 * This class reads in a nutch page, extracts out the links, and
 * foreach link:
 *   emits (currPage, (linkedPage, 1))
 *
 *
 */
public class GraphBuilderMapper extends MapReduceBase implements Mapper {

  private static void debug(String s) {
	  System.out.println(s);
  }

  public void map(WritableComparable key, Writable value,
                  OutputCollector output, Reporter reporter) throws IOException {
    // Prepare the input data.
    String page = value.toString();
    
    // debug
    debug("Page:" + page);

    String title = this.GetTitle(page, reporter);
    if (title.length() > 0) {
      reporter.setStatus(title);
    } else {
      return;
    }

    // debug
    debug("Title = " + title);
    
    ArrayList<String> outlinks = this.GetOutlinks(page);
    StringBuilder builder = new StringBuilder();
    for (String link : outlinks) {
      link = link.trim().replace(" ", "_");
      builder.append(" ");
      builder.append(link);
    }

    // debug
    debug("Builder = " + builder.toString());

    output.collect(new Text(title), new Text(builder.toString()));
  }

  public String GetTitle(String page, Reporter reporter) throws IOException{
    int start = page.indexOf("&lt;title&gt;")+13;
    if (start == 12) {
      reporter.setStatus("couldn't find start");
      return "";
    }
    int end = page.indexOf("&lt;/title&gt;", start);
    if (end == -1) {
      reporter.setStatus("Couldn't find end");
      return "";
    }
    
    String title = page.substring(start);
    title = title.substring(0, end-start);
    reporter.setStatus("Found title " + title);
    return title;
  }

  public ArrayList<String> GetOutlinks(String page){
    int end;
    ArrayList<String> outlinks = new ArrayList<String>();
    int start=page.indexOf("[[");
    while (start > 0) {
      start = start+2;
      end = page.indexOf("]]", start);
      //if((end==-1)||(end-start<0))
      if (end == -1) {
        break;
      }

      String toAdd = page.substring(start);
      toAdd = toAdd.substring(0, end-start);
      outlinks.add(toAdd);

      // debug      
      System.out.println("Article = " + toAdd);

      start = page.indexOf("[[", end+1);
    }
    return outlinks;
  }
}
