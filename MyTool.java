package mypackage;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class MyTool {
	public static String mapkey2String(HashMap<String,Integer>map, String separator){
		String res = "";
		List<String> keys = new ArrayList<String>(map.keySet());
		Collections.sort(keys, new MyTool.CompareMap1(map));
		for(int i=0;i<keys.size();i++){
			String key = keys.get(i);
			res = res + key + separator;
		}
		return res;
	}
	public static String lcs(String a, String b) {
	    int[][] lengths = new int[a.length()+1][b.length()+1];
	    // row 0 and column 0 are initialized to 0 already
	    for (int i = 0; i < a.length(); i++)
	        for (int j = 0; j < b.length(); j++)
	            if (a.charAt(i) == b.charAt(j))
	                lengths[i+1][j+1] = lengths[i][j] + 1;
	            else
	                lengths[i+1][j+1] =
	                    Math.max(lengths[i+1][j], lengths[i][j+1]);
	    // read the substring out from the matrix
	    StringBuffer sb = new StringBuffer();
	    for (int x = a.length(), y = b.length();
	         x != 0 && y != 0; ) {
	        if (lengths[x][y] == lengths[x-1][y])
	            x--;
	        else if (lengths[x][y] == lengths[x][y-1])
	            y--;
	        else {
	            assert a.charAt(x-1) == b.charAt(y-1);
	            sb.append(a.charAt(x-1));
	            x--;
	            y--;
	        }
	    }
	    return sb.reverse().toString();
	}
	public static List<String> SystemReadInFile(String filepath) {
		return SystemReadInFile(filepath, "utf-8");
	}
	public static List<String> SystemReadInFile(String filepath, String ENCODE) {
		ArrayList<String> lines = new ArrayList<String>();
		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(new FileInputStream(filepath), ENCODE));
			String line = null;
			while ((line = br.readLine()) != null) {
				lines.add(line);
			}
			br.close();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return lines;
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
			System.err.println("File Not Found Exception.");
			return lines;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return lines;
		}
		return lines;
	}
	public static class CompareMap implements Comparator<String> {
		protected HashMap<String, Long> map;

		public CompareMap(HashMap<String, Long> map) {
			this.map = new HashMap<String, Long>(map);
		}

		@Override
		public int compare(String o1, String o2) {
			// TODO Auto-generated method stub
			long a = 0, b = 0;
			a = map.get(o1);
			b = map.get(o2);
			if (a < b)
				return 1;
			else if (a == b)
				return 0;
			else
				return -1;
		}
	}
	public static class CompareMap1 implements Comparator<String> {
		protected HashMap<String, Integer> map;

		public CompareMap1(HashMap<String, Integer> map) {
			this.map = new HashMap<String, Integer>(map);
		}

		@Override
		public int compare(String o1, String o2) {
			// TODO Auto-generated method stub
			long a = 0, b = 0;
			a = map.get(o1);
			b = map.get(o2);
			if (a < b)
				return 1;
			else if (a == b)
				return 0;
			else
				return -1;
		}
	}
	public static class CompareMyNewsPV implements Comparator<String> {
		protected HashMap<String, MyNews> map;

		public CompareMyNewsPV(HashMap<String, MyNews> map) {
			this.map = new HashMap<String, MyNews>(map);
		}

		@Override
		public int compare(String o1, String o2) {
			// TODO Auto-generated method stub
			MyNews n1 = new MyNews("url","title","209901010101","0");
			MyNews n2 = new MyNews("url","title","209901010101","0");
			int a = 0,b=0;
			a = n1.pv;
			b = n2.pv;
			
			if (a < b)
				return 1;
			else if (a == b)
				return 0;
			else
				return -1;
		}
	}
}
