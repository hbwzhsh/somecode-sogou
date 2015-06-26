package mypackage;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;



public class BucketNewsParse {
	private static BufferedReader buffReader;
	private BufferedReader buffReader1;
	public void test() throws IOException{
		buffReader1 = new BufferedReader (new FileReader(""));
		String line = buffReader1.readLine();
		int count = 0;
		int error = 0;
		String url="";
		String title="";
		String topics = "";
        while(line != null){
        	try{
        		if(!line.isEmpty()){
        			News news = JSON.parseObject(line,News.class);
            		if(news!=null){
            			url = news.getUrl().trim();
            			title = news.getTitle().trim();
            			topics = news.getBudgets_jin().trim();
            			System.out.println(url);
            	        System.out.println(title);
            	        System.out.println(topics);
            		}
        		}
        	}catch(JSONException ne){
        		error++;
        		if(error == 1)
        		{
        			System.out.println(line);
        			System.out.println(ne);
        		}
        		
        	}
        	
        	line = buffReader.readLine();
        	count++;
        }
        System.out.println(count);
        System.out.println(error);
	}
	public void v1(String[] argv) throws IOException{
		File[] files = new File(argv[0]).listFiles();
		BufferedWriter writer = new BufferedWriter(new FileWriter(argv[1]));
		int count = 0;
		int error = 0;
		String url = "";
		String title = "";
		String topics = "";
		for (File file : files) {
			if(file.isFile()&&file.getName().contains("success")){
				buffReader = new BufferedReader (new FileReader(file.getAbsolutePath()));
				System.out.println(file.getAbsolutePath());
		        String line = buffReader.readLine();
		        while(line != null){
		        	try{
		        		if(!line.isEmpty()){
		        			News news = JSON.parseObject(line,News.class);
		            		if(news!=null){
		            			url = news.getUrl().trim();
		            			title = news.getTitle().trim();
		            			topics = news.getBudgets_jin().trim();
		            			writer.write(url+"\t"+title+"\t"+topics+"\n");
		            		}
		        		}
		        	}catch(JSONException ne){
		        		error++;
		        	}
		        	count++;
		        	if(count%10000==0)
		        		System.out.println(count);
		            line = buffReader.readLine();
		        }
			}
		}
		System.out.println("ERROR:"+Integer.toString(error));
		writer.close();
	}
	public static void main(String[] argv) throws IOException{
		buffReader = new BufferedReader (new FileReader(argv[0]));
		BufferedWriter writer = new BufferedWriter(new FileWriter(argv[1]));
		int count = 0;
		int error = 0;
		String url = "";
		String title = "";
		String topics = "";
        String line = buffReader.readLine();
        while(line != null){
        	try{
        		if(!line.isEmpty()){
        			News news = JSON.parseObject(line,News.class);
        			url = "";
        			title = "";
        			topics = "";
            		if(news!=null){
            			url = news.getUrl().trim();
            			title = news.getTitle().trim();
            			topics = news.getBudgets_jin().trim();
            			if( (!url.isEmpty()) && (!title.isEmpty()) &&(!topics.isEmpty()) )
            				writer.write(url+"\t"+title+"\t"+topics+"\n");
            		}
        		}
        	}catch(JSONException ne){
        		error++;
        	}
        	count++;
        	if(count%10000==0)
        		System.out.println(count);
            line = buffReader.readLine();
        }
		System.out.println("ERROR:"+Integer.toString(error));
		writer.close();
	}
}
