package mypackage;

import java.io.IOException;
//import java.util.ArrayList;
//import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
//import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;
//import java.util.regex.Matcher;
//import java.util.regex.Pattern;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.wltea.analyzer.Lexeme;

import org.wltea.analyzer.dic.Dictionary;

//import com.sogou.iportalnews.download.SearchKey;
import com.sogou.mapreduce.lib.input.CombineFileMultiTextInputFormat;
import com.sohu.bright.wordseg.IKSegmentor;

public class PostVerticalFenci{
	public static class MyMap1 extends Mapper<Writable, Text, Text, Text> {
		HashSet<String> dict_set = new HashSet<String>();
		static IKSegmentor ikSegmentor = new IKSegmentor();
		private void loadDitc(String ptfile) {
			for (String line : MyTool.SystemReadInFile(ptfile)) {
				String[] tokens = line.split("\t");
				dict_set.add(tokens[0]);
			}
		}
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			loadDitc(conf.get("MR.allwordlistmore.data"));
			ikSegmentor.initialize("mydict");
			Dictionary.loadExtendWords(dict_set);
		}
		protected void fenci(String s, HashMap<String, Integer> m_fenci,int times,int Limit){
			Lexeme[] s_arr = ikSegmentor.segment(s);
	    	if(s_arr!=null)
	    	{
	    		for(int i=0;i<s_arr.length;i++)
	    		{
	    			String temp = s_arr[i].getLexemeText();
	    			if(temp.length()>1&&dict_set.contains(temp)){
	    				if(m_fenci.containsKey(temp)){
	    					m_fenci.put(temp, m_fenci.get(temp)+1*times);
	    				}
	    				else
	    					m_fenci.put(temp, 1*times);
	    			}
	    		}
			}
	        return;
		}
		public static TreeMap<String, Integer> SortByValue(HashMap<String, Integer> map) {
			ValueComparator vc =  new ValueComparator(map);
			TreeMap<String,Integer> sortedMap = new TreeMap<String,Integer>(vc);
			sortedMap.putAll(map);
			return sortedMap;
		}
		@Override
		protected void map(Writable key, Text value, Context context)throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\t");
			if (tokens.length < 4)
				return;
			String query = tokens[0].replaceAll("\t", " ").trim();
			String sum = tokens[1].replaceAll("\t", " ").trim();
			String topics = tokens[2].replaceAll("\t", " ").trim();
			int Limit = 10;
			HashMap<String, Integer> m_fenci = new HashMap<String,Integer>();
			HashMap<String, Integer> m_title_fenci = new HashMap<String,Integer>();
			//fenci Query
			fenci(query,m_fenci,Integer.parseInt(sum),Limit);
			//fenci titles
			for(int i=3;i<tokens.length;i++){
				String temp_title = tokens[i];
				String[] splits = temp_title.split(":::");
				int temp_sum = 0;
				if(splits.length>1){
					String s2i = splits[splits.length-1].replaceAll(":", "");
					temp_sum = Integer.parseInt(s2i);
				}
				fenci(temp_title,m_title_fenci,temp_sum,Limit);
			}
			
			//sort and construct output strings
			TreeMap<String, Integer> sortedMap = SortByValue(m_fenci);
    	    Set<Entry<String, Integer>> set = sortedMap.entrySet();
    	    Iterator<Entry<String, Integer>> i = set.iterator();
    	    int count = 0;
    	    String title_fenci = "";
    	    while(i.hasNext()&&count<Limit) {
    			Entry<String, Integer> me = i.next();
    			title_fenci = title_fenci + me.getKey().toString()+":"+me.getValue().toString()+",";
    			count++;
    	    }
    	    
    	    // title fenci
    	    String query_fenci = "";
    	    sortedMap = SortByValue(m_title_fenci);
    	    set = sortedMap.entrySet();
    	    i = set.iterator();
    	    count = 0;
    	    while(i.hasNext()&&count<Limit) {
    			Entry<String, Integer> me = i.next();
    			query_fenci = query_fenci + me.getKey().toString()+":"+me.getValue().toString()+",";
    			count++;
    	    }
			
			String value_string = sum+"\t"+topics+"\t"+query_fenci+"\t"+title_fenci+"\t";
			for(int j=3;j<tokens.length;j++){
				value_string = value_string + tokens[j]+"\t";
			}
			context.write(new Text(query), new Text(value_string));
		}
	}

	public static class MyReduce1 extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				context.write(key,value);
			}
		}
	}

	public void run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		GenericOptionsParser goparser = new GenericOptionsParser(conf, args);
		args = goparser.getRemainingArgs();
		FileSystem fs = FileSystem.get(conf);
		
		String dictFile = args[3];
		String dictFileBN = StringUtil.baseName(dictFile);
		JobBase.setJobFileInConf(conf, dictFile);
		conf.set("MR.allwordlistmore.data", dictFileBN);

		String ptFile = args[2];
		String ptFileBN = StringUtil.baseName(ptFile);
		JobBase.setJobFileInConf(conf, ptFile);
		conf.set("MR.pt.data", ptFileBN);

		String today = args[1];
		conf.set("MR.today", today);
		Path inpath = new Path("/user/ipt/hujin/QN_vertical." + today);

		String filename = this.getClass().getSimpleName();
		String outfile = args[0] + filename + "." + today;
		Path outpath = new Path(outfile);

		Job job;
		if (fs.exists(outpath)) {
			System.out.println("The output file exist!" + "\n\tRemoving "
					+ outpath);
			fs.delete(outpath, true);
		}
		job = new Job(conf, "QN PostVerticalFenci@Hujin");
		job.setJarByClass(PostVerticalFenci.class);
		job.setMapperClass(MyMap1.class);
		job.setReducerClass(MyReduce1.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(CombineFileMultiTextInputFormat.class);
		job.setInputFormatClass(CombineFileMultiTextInputFormat.class);

		job.setNumReduceTasks(50);
		FileInputFormat.addInputPath(job, inpath);
		FileOutputFormat.setOutputPath(job, outpath);
		job.waitForCompletion(true);

	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		new PostVerticalFenci().run(args);
	}
}
