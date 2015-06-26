package mypackage;
import java.io.IOException;
import java.io.StringReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.oro.text.regex.MalformedPatternException;
import org.apache.oro.text.regex.PatternCompiler;
import org.apache.oro.text.regex.PatternMatcher;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;

/*
 * @author: Mayan
 */
public class StringUtil {
	public static HashMap<String, String> htmlCharMap = new HashMap<String, String>();
	static
	{
		htmlCharMap.put("&lt;", "<");
		htmlCharMap.put("&gt;", ">");
		htmlCharMap.put("&amp;", "&");
		htmlCharMap.put("&quot;", "\"");
		htmlCharMap.put("&nbsp;", " ");
	}

	public static boolean isOnlyAscii(String str) {
		for (char ch : str.toCharArray()) {
			if ((int) ch >= 128) {
				return false;
			}
		}
		return true;
	}

	public static String trimString(String in) {
		if (in == null) {
			return "";
		} else {
			return in.trim();
		}
	}

	public static boolean isStringEmpty(String in) {
		return (in == null || in.trim().equals(""));
	}

	public static String SubString(String input, String start_str,
			String end_str) {
		int start_pos = input.indexOf(start_str);
		if (start_str.equals("")) {
			start_pos = 0;
		}
		if (start_pos == -1)
			return "";

		String output = null;
		int end_pos = input.indexOf(end_str, start_pos + 1);
		if (end_str.equals("")) {
			end_pos = -1;
		}

		if (end_pos == -1) {
			output = input.substring(start_pos + start_str.length());
		} else {
			output = input.substring(start_pos + start_str.length(), end_pos);
		}

		return output;
	}

	public static String removeSubString(String input, String start_str,
			String end_str) {
		String output = input, left, right;
		int start_pos = input.indexOf(start_str);
		if (start_pos == -1)
			return input;

		int end_pos = input.indexOf(end_str, start_pos + 1);
		if (end_pos == -1) {
			left = input.substring(0, start_pos);
			right = "";
		} else {
			left = input.substring(0, start_pos);
			right = input.substring(end_pos + end_str.length());
		}
		output = left + right;

		return output;
	}

	public static String removeAllSubString(String input, String start_str,
			String end_str) {
		if (input == null || start_str == null || end_str == null)
		{
			return "";
		}
		start_str = start_str.toLowerCase();
		end_str = end_str.toLowerCase();
		String result = input.replaceAll("(?i)" + start_str, start_str);
		result = result.replaceAll("(?i)" + end_str, end_str);

		String output = result;

		while (output.indexOf(start_str) >= 0) {
			output = removeSubString(output, start_str, end_str);
		}

		return output;
	}

	public static String UnicodeToString(String str) {
		Pattern pattern = Pattern.compile("(\\\\u(\\p{XDigit}{4}))");
		Matcher matcher = pattern.matcher(str);
		char ch;
		while (matcher.find()) {
			ch = (char) Integer.parseInt(matcher.group(2), 16);
			str = str.replace(matcher.group(1), ch + "");
		}
		return str;
	}

	public static ArrayList<String> getStringList(String Text,
			String StartKeyWord, String EndKeyWord) {
		ArrayList<String> ResultList = new ArrayList<String>();
		int start_pos = Text.indexOf(StartKeyWord);

		if (start_pos == -1)
			return null;

		String output = null;
		while (start_pos != -1) {
			int end_pos = Text.indexOf(EndKeyWord, start_pos + 1);
			if (end_pos == -1) {
				output = Text.substring(start_pos + StartKeyWord.length());
				start_pos = -1;
			} else {
				output = Text.substring(start_pos + StartKeyWord.length(),
						end_pos);
				start_pos = Text.indexOf(StartKeyWord, end_pos + 1);
			}
			ResultList.add(output);
		}

		if (ResultList.size() > 0) {
			return ResultList;
		} else {
			return null;
		}
	}

	public static ArrayList<String> SubStringList(ArrayList<String> StringList,
			String StartKeyWord, String EndKeyWord) {
		for (int i = 0; i < StringList.size(); i++) {
			String StringItem = StringList.get(i);
			StringItem = SubString(StringItem, StartKeyWord, EndKeyWord);
			StringList.set(i, StringItem);
		}

		return StringList;
	}

	public static HashMap<String, String> get_key_value_map(String input) {
		HashMap<String, String> keyval_map = new HashMap<String, String>();
		String[] items = input.split("\n");

		for (String item : items) {
			item = StringUtil.trimString(item);
			int pos = item.indexOf(":");
			if (pos == -1)
				continue;

			String key = item.substring(0, pos);
			String val = item.substring(pos + 1);
			key = StringUtil.SubString(key, "\"", "\"");
			val = StringUtil.SubString(val, "\"", "\"");
			key = StringUtil.trimString(key);
			val = StringUtil.trimString(val);
			keyval_map.put(key, val);
		}

		return keyval_map;
	}

	public static HashMap<String, String> getKeyValueHashMap(String input,
			String spliter, String LF) {
		HashMap<String, String> keyValueMap = new HashMap<String, String>();
		String[] items = input.split(LF);

		for (String item : items) {
			item = StringUtil.trimString(item);
			int pos = item.indexOf(spliter);
			if (pos == -1)
				continue;

			String key = item.substring(0, pos);
			String val = item.substring(pos + 1);
			key = StringUtil.trimString(key);
			val = StringUtil.trimString(val);

			keyValueMap.put(key, val);
		}

		return keyValueMap;
	}

	public static String MyTrimString(String input) {
		String[] FilterStringArray = new String[] { "[", "]" };

		for (int i = 0; i < FilterStringArray.length; i++) {
			input = input.replace(FilterStringArray[i], "");
		}

		return input;
	}

	public static int[] getNumberArray(String input) {
		String digitString;
		ArrayList<Integer> numberList = new ArrayList<Integer>();
		int beginPos = -1;
		int endPos = -1;

		beginPos = (input.indexOf("2012") >=0) ?  input.indexOf("2012") : 0;
		for (int i = beginPos; i < input.length(); i++) {
			char ch = input.charAt(i);
			if (ch < '0' || ch > '9') {
				if (beginPos >= 0) {
					endPos = i;
					digitString = input.substring(beginPos, endPos);
					numberList.add(new Integer(digitString));
					beginPos = -1;
				}
				continue;
			} else if (beginPos < 0) {
				beginPos = i;
			}
		}

		if (beginPos >= 0) {
			digitString = input.substring(beginPos);
			numberList.add(new Integer(digitString));
		}

		int[] numberArray = new int[numberList.size()];
		for (int i = 0; i < numberList.size(); i++) {
			numberArray[i] = numberList.get(i).intValue();
		}

		return numberArray;
	}

	public static long converToTimeSecond(int[] dateNumbers) {
		int year = 0, month = 0, day = 0;
		int hour = 0, minute = 0, second = 0;

		for (int i = 0; i < dateNumbers.length; i++) {
			switch (i) {
			case 0:
				year = dateNumbers[i] - 1900;
				break;
			case 1:
				month = dateNumbers[i] - 1;
				break;
			case 2:
				day = dateNumbers[i];
				break;
			case 3:
				hour = dateNumbers[i];
				break;
			case 4:
				minute = dateNumbers[i];
				break;
			case 5:
				second = dateNumbers[i];
				break;
			default:
				break;
			}
		}

		@SuppressWarnings("deprecation")
		Date date = new Date(year, month, day, hour, minute, second);
		return date.getTime();
	}

	public static boolean isNum(String s) {
		try {
			Double.parseDouble(s);
		} catch (Exception e) {
			return false;
		}

		return true;
	}

	public static boolean isDateString(String input) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		try {
			sdf.parse(input);
		} catch (ParseException e) {
			return false;
		}

		return true;
	}

	public static String removeHtmlTag(String html, String tagName) {
		String s = "<" + tagName;
		String e = ">";
		String tmpString = html;
		tmpString = removeAllSubString(tmpString, s, e);
		String suffix = "(?i)</" + tagName + ">";
		tmpString = tmpString.replaceAll(suffix, "");

		return tmpString;
	}
	
	public static boolean regexStringCompare(String patternString, String hack)
	{
		try {
			PatternCompiler orocom = new Perl5Compiler();
			org.apache.oro.text.regex.Pattern pattern =  orocom.compile(patternString);
			PatternMatcher matcher = new Perl5Matcher();
			return matcher.contains(hack, pattern);
			
		} catch (MalformedPatternException e) {
			return false;
		}
	}

	public static String printXmlString(String prefix, String content)
	{
		String tmpString = "<"+prefix+">\n" + content + "\n</"+prefix+">";
		
		return tmpString;
	}
	
	public static String baseName(String fileName)
	{
		String basename = fileName;
		int pos = fileName.lastIndexOf("/");
		if (pos >= 0)
		{
			basename = fileName.substring(pos+1);
		}
			
		return basename;
	}
	
	public static String implode(ArrayList<String> strings, String glue)
	{
		StringBuilder sb = new StringBuilder();
		if (strings == null || strings.size() <= 0)	return null;
		
		sb.append(strings.get(0));
		for (int i=1; i<strings.size(); i++)
		{
			sb.append(glue);
			sb.append(strings.get(i));
		}
		
		return sb.toString();
	}
	
	public static String unescapeHtml(String input)
	{
		String output = input;
		Set<String> keySet = htmlCharMap.keySet();
		for(String key : keySet)
		{
			String replacement = htmlCharMap.get(key);
			output = output.replace(key, replacement);
		}
		
		return output;
	}
	
	 public static void main(String args[])
	 {
		 String input = "abcde&nbsp;fff";
		 String out = unescapeHtml(input);
		 System.out.println("out="+out);
	 }
}
