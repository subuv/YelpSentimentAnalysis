
import java.io.*;
import java.util.*;

import org.json.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class YelpMapper extends Mapper<LongWritable, Text, Text, Text> {
	//The path to the stopWords file.         
	final static String STOP_WORD_FILE = "/usr/local/JarFiles/yelp/stopWords"; 
	String[] tokenArray;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		//Removing the stop words. You need to complete this part
		Scanner stp_file = new Scanner(new File(STOP_WORD_FILE)).useDelimiter("\n");
		List<String> tokens = new ArrayList<String>();
	    while (stp_file.hasNext()) {
	        tokens.add(stp_file.nextLine());
	    }
	    tokenArray = tokens.toArray(new String[0]);
    }

	@Override
	public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
		JSONObject yelp_json;
		try {
			//Extracting business_id and text fields from the json object
			yelp_json = new JSONObject(value.toString());
		
			String text = (String)yelp_json.get("text");
			String business_id = (String)yelp_json.get("business_id");
			String review = "";
		    for (String s : tokenArray) {
		    	review += text.replace(s, "");
		    }
		    context.write(new Text(business_id), new Text(review));
		}catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
