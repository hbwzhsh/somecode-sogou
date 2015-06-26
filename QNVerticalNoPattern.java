package mypackage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
//import java.util.Iterator;
import java.util.List;
import java.util.Set;
//import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;
//import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.wltea.analyzer.Lexeme;


import org.wltea.analyzer.dic.Dictionary;

import com.sogou.iportalnews.download.SearchKey;
import com.sogou.mapreduce.lib.input.CombineFileMultiTextInputFormat;
import com.sohu.bright.wordseg.IKSegmentor;


//currently only keep reduce results with fenci from both query and title
// alternative: keep all the results either with fenci or not
public class QNVerticalNoPattern{
	public static class MyMap1 extends Mapper<Writable, Text, Text, Text> {
		public Pattern keyPattern = Pattern
				.compile("(\\?|&|#)(query|q|q1|wd|word|search_text|keyword|kw|key|lq|sp)(=)([^&\\?#]+)");
		@Override
		protected void map(Writable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\t");
			if (tokens.length < 5)
				return;
			String url = tokens[2].replaceAll("\t", " ").trim();
			if(url.length()==0 || url.contains("news.baidu.com") || url.contains("news.sogou.com"))
				return;
			String title = tokens[3].replaceAll("\t", " ").trim();
			if(title.length()==0)
				title = "-";
			String refer = tokens[4];
			String keyword = "";
			Matcher keyMatcher = keyPattern.matcher(refer);
			if(refer.contains("news.baidu.com") || refer.contains("news.sogou.com")){
				if(keyMatcher.find()){
					try {
						refer = refer.replaceAll("\\+cont:[0-9]+", "");
						keyword = SearchKey.extractkey(refer).replaceAll("\t", " ").trim();
						if (keyword == null)
							return;
						context.write(new Text(keyword), new Text(url + "\t" + title));
					} catch (Exception e) {
					}
				}
			}
		}
	}

	public static class MyReduce1 extends Reducer<Text, Text, Text, Text> {
		HashSet<String> dict_set = new HashSet<String>();
		public static org.apache.hadoop.mapreduce.Counter ct = null;
		static IKSegmentor ikSegmentor = new IKSegmentor();
		public static TreeMap<String, Integer> SortByValue(HashMap<String, Integer> map) {
			ValueComparator vc =  new ValueComparator(map);
			TreeMap<String,Integer> sortedMap = new TreeMap<String,Integer>(vc);
			sortedMap.putAll(map);
			return sortedMap;
		}
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			loadDitc(conf.get("MR.allwordlistmore.data"));
			ikSegmentor.initialize("mydict");
			Dictionary.loadExtendWords(dict_set);
		}
		private void loadDitc(String ptfile) {
			for (String line : MyTool.SystemReadInFile(ptfile)) {
				String[] tokens = line.split("\t");
				dict_set.add(tokens[0]);
			}
		}
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			int Limit = 2;
			String query = key.toString();
			HashMap<String, Integer> m_query_pv = new HashMap<String, Integer>();
			for (Text value : values) {
				sum++;
				String news_url_title = value.toString();
				if (!m_query_pv.containsKey(news_url_title))
					m_query_pv.put(news_url_title, 0);
				m_query_pv.put(news_url_title, m_query_pv.get(news_url_title) + 1);
			}
			if (sum < 1)
				return;
			List<String> news_array = new ArrayList<String>(m_query_pv.keySet());
			Collections.sort(news_array, new MyTool.CompareMap1(m_query_pv));
			
			// fenci Query and news titles below
			HashMap<String, Integer> m_query_fenci = new HashMap<String, Integer>();
			HashMap<String, Integer> m_titles_fenci = new HashMap<String, Integer>();
			
			Lexeme[] query_arr = ikSegmentor.IKSegment(query,true);
			for(int i=0;i<query_arr.length;i++)
			{
				String temp = query_arr[i].getLexemeText();
				if(temp.length()>1)
					m_query_fenci.put(temp, sum);
			}
			for (String news : news_array) {
				if (m_query_pv.get(news) < 1 || news.isEmpty())
					continue;
				String[] splits = news.split("\t");
				if(splits.length>=2){
					String url = splits[0].trim();
					String title = splits[1].trim();
					Lexeme[] title_arr = ikSegmentor.IKSegment(title,true);
					for(int i=0;i<title_arr.length;i++)
					{
						String temp = title_arr[i].getLexemeText();
						if(temp.length()>1){
							if (!m_titles_fenci.containsKey(temp))
								m_titles_fenci.put(temp, 0);
							m_titles_fenci.put(temp, m_titles_fenci.get(temp) + 1);
						}
					}
				}
			}
			// combine two fenci results and merge
			TreeMap<String, Integer> sortedMap = SortByValue(m_query_fenci);
    	    Set<Entry<String, Integer>> set = sortedMap.entrySet();
    	    Iterator<Entry<String, Integer>> i = set.iterator();
    	    HashMap<String, Integer> result_fenci = new HashMap<String, Integer>();
    	    while(i.hasNext()){
    			Entry<String, Integer> me = i.next();
    			if(m_titles_fenci.containsKey(me.getKey()))
    				result_fenci.put(me.getKey(), me.getValue()+m_titles_fenci.get(me.getKey()));
    	    }
    	    sortedMap = SortByValue(result_fenci);
    	    set = sortedMap.entrySet();
    	    i = set.iterator();
    	    int count = 0;
    	    String fenci_string = "";
    	    int VERYLARGEINTEGER = 1000000000;
    	    while(i.hasNext()&&count<Limit){
    			Entry<String, Integer> me = i.next();
    			fenci_string = fenci_string + me.getKey()+":"+me.getValue()+" ";
    			if(me.getValue()<VERYLARGEINTEGER){
    				count = count + 1;
    				VERYLARGEINTEGER = me.getValue();
    			}
    	    }
    	    StringBuilder sb = new StringBuilder();
    	    sortedMap = SortByValue(m_query_pv);
    	    set = sortedMap.entrySet();
    	    i = set.iterator();
    	    while(i.hasNext()){
    			Entry<String, Integer> me = i.next();
    			String temp = me.getKey().replaceAll("\t", " ");
    			sb.append(temp+":::"+me.getValue()+"\t");
    	    }
			//result
    	    if(!fenci_string.equals(""))
    	    	context.write(key,new Text(sum+"\t"+fenci_string+"\t"+sb.toString()));
		}
	}

	public void run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		GenericOptionsParser goparser = new GenericOptionsParser(conf, args);
		args = goparser.getRemainingArgs();
		FileSystem fs = FileSystem.get(conf);
		
		String dictFile = args[2];
		String dictFileBN = StringUtil.baseName(dictFile);
		JobBase.setJobFileInConf(conf, dictFile);
		conf.set("MR.allwordlistmore.data", dictFileBN);
		conf.set("mapred.job.priority", JobPriority.HIGH.toString());
		String today = args[1];
		conf.set("MR.today", today);
		Path inpath = new Path("/user/ipt/recom/raw_data/" + today);

		String filename = this.getClass().getSimpleName();
		String outfile = args[0] + filename + "." + today;
		Path outpath = new Path(outfile);

		Job job;
		if (fs.exists(outpath)) {
			System.out.println("The output file exist!" + "\n\tRemoving "
					+ outpath);
			fs.delete(outpath, true);
		}
		job = new Job(conf, "QNVerticalNoPattern@Hujin");
		job.setJarByClass(QNVerticalNoPattern.class);
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
		new QNVerticalNoPattern().run(args);
	}
}
