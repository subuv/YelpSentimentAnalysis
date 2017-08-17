import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class YelpReducer extends Reducer<Text,Text,Text,Text> {
  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    int count = 0;
    String reviews = ""; 
    //Summing up the counts for each word
    for (Text value : values) {
      reviews += " " + value.toString();
      count++;
    }
    if(count>=50) {
      context.write(key, new Text(reviews));
    }
  }
}