package mypackage;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.simple.JSONObject;

import com.sogou.iportalnews.download.SearchKey;

//run on intermediate data which is in format: "new_title \t query_link"

public class NQOnIntermediate {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
	    public void map(LongWritable key, Text text, Context context) throws IOException, InterruptedException {
	    	String line = text.toString();
	    	String[] s_array = line.split("\t");
	    	if(s_array.length<2)
	    		return;
	    	try{
	    		String temp = SearchKey.extractkey_utf8only(s_array[1]);
	    		// with search query
	    		if(temp!=null){
    				Text out1 = new Text(s_array[0]);
    		    	Text out2 = new Text(temp);
    		        context.write(out1, out2);
    		        return;
	    		}
	    		else{
    				Text out1 = new Text(s_array[0]);
    		    	Text out2 = new Text("-");
    		        context.write(out1, out2);
    		        return;
	    		}
	    	}
	    	catch(IllegalArgumentException e){
	    		return;
	    	}
	    }
	}
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
	    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	    	HashMap<String, Integer> hm = new HashMap<String, Integer>();
	    	for (Text val : values) {
	    		String query = val.toString();
	        	if(hm.containsKey(query)){
	        		hm.put(query, hm.get(query) + 1);
	        	}
	        	else{
	        		hm.put(query, 1);
	        	}
	        }
	    	TreeMap<String, Integer> sortedMap = SortByValue(hm);
	    	Set<Entry<String, Integer>> set = sortedMap.entrySet();
	 	    Iterator<Entry<String, Integer>> i = set.iterator();
	 	    JSONObject obj = new JSONObject();
	 	    while(i.hasNext()) {
	 			Entry<String, Integer> me = i.next();
	 			String key_string = me.getKey().toString();
	 			obj.put(key_string, me.getValue().toString());
	 	    }
	        context.write(key,new Text(obj.toJSONString()));
	    }
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
	    Job job = new Job(conf, "NQ_On_IntermediateData@hujin.experiment");
	    job.setJarByClass(NQOnIntermediate.class);
	    job.setMapperClass(Map.class);
	    job.setReducerClass(Reduce.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.waitForCompletion(true);
	 }
	public static TreeMap<String, Integer> SortByValue(HashMap<String, Integer> map) {
		ValueComparator vc =  new ValueComparator(map);
		TreeMap<String,Integer> sortedMap = new TreeMap<String,Integer>(vc);
		sortedMap.putAll(map);
		return sortedMap;
	}
}