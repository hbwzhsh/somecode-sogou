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

import com.sogou.iportalnews.download.SearchKey;

//input intermediate data format: category \t title \t url \t search_query_url
//output format: category \t title count query:count query:count ...

public class NQbyCategory {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
	    public void map(LongWritable key, Text text, Context context) throws IOException, InterruptedException {
	    	String line = text.toString();
	    	String[] s_array = line.split("\t");
	    	if(s_array.length<4)
	    		return;
	    	try{
	    		String temp = SearchKey.extractkey_utf8only(s_array[3]);
	    		// with search query
	    		if(temp!=null){
    				Text out1 = new Text(s_array[0]+"\t"+s_array[1]);
    		    	Text out2 = new Text(temp);
    		        context.write(out1, out2);
    		        return;
	    		}
	    		else{
    				Text out1 = new Text(s_array[0]+"\t"+s_array[1]);
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
	 	    String output_string = "";
	 	    while(i.hasNext()) {
	 			Entry<String, Integer> me = i.next();
	 			output_string = output_string + me.getKey().toString() + ":" + me.getValue().toString() + "\t";
	 	    }
	        context.write(key,new Text(output_string));
	    }
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
	    Job job = new Job(conf, "NQ_On_IntermediateData2@hujin.experiment");
	    job.setJarByClass(NQbyCategory.class);
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