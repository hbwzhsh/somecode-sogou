/**
 * 
 */
package mypackage;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author ATP
 * 
 */
/**
 * @author ATP
 *
 */
/**
 * @author ATP
 * 
 */
public class URLMisc {

	public static final int Max_URL_Length = 256;

	/**
	 * 搜索引擎域名
	 */
	protected static final HashSet<String> SearchEngineDomain;

	/**
	 * 搜索引擎对应的query参数
	 */
	protected static final HashMap<String, String[]> SearchEngineQuery;
	// /**
	// * 使用UTF的搜索引擎
	// */
	// protected static final HashSet<String> UTFSearchQuery;
	/**
	 * 顶级域名列表
	 */
	protected static final HashSet<String> TopLevelDomain;
	/**
	 * 国家域名列表
	 */
	protected static final HashSet<String> CountryDomain;
	/**
	 * Navigation URLs
	 */
	protected static final String[] NavigationURLs;
	
	/**
	 * shoping domain
	 */
	protected static final HashSet<String> Shopping;
	
	/**
	 * video domain
	 */
	protected static final HashSet<String> Video;

	static {
		SearchEngineDomain = new HashSet<String>(Arrays.asList("sogou",
				"google", "baidu", "youdao", "yodao", "bing", "soso", "gougou"));

		SearchEngineQuery = new HashMap<String, String[]>();
		SearchEngineQuery.put("google", new String[] { "q=" });
		SearchEngineQuery.put("sogou", new String[] { "query=" });
		SearchEngineQuery.put("baidu", new String[] { "wd=", "word=", "kw=" });
		SearchEngineQuery.put("bing", new String[] { "q=" });
		SearchEngineQuery.put("soso", new String[] { "w=" });
		SearchEngineQuery.put("youdao", new String[] { "q=" });
		SearchEngineQuery.put("yodao", new String[] { "q=" });
		SearchEngineQuery.put("gougou", new String[] { "search=" });

		// UTFSearchQuery = new HashSet<String>(Arrays.asList("google", "bing",
		// "yodao", "youdao"));

		TopLevelDomain = new HashSet<String>(Arrays.asList("ac", "aero",
				"arpa", "asia", "biz", "cat", "com", "coop", "edu", "gov",
				"info", "int", "jobs", "mi", "mobi", "museum", "name", "net",
				"org", "pro", "te", "trave", "co"));

		CountryDomain = new HashSet<String>(Arrays.asList("ad", "ae", "af",
				"ag", "ai", "a", "am", "an", "ao", "aq", "ar", "as", "at",
				"au", "aw", "ax", "az", "ba", "bb", "bd", "be", "bf", "bg",
				"bh", "bi", "bj", "bm", "bn", "bo", "br", "bs", "bt", "bv",
				"bw", "by", "bz", "ca", "cc", "cd", "cf", "cg", "ch", "ci",
				"ck", "c", "cm", "cn", "co", "cr", "cu", "cv", "cx", "cy",
				"cz", "de", "dj", "dk", "dm", "do", "dz", "ec", "ee", "eg",
				"er", "es", "et", "eu", "fi", "fj", "fk", "fm", "fo", "fr",
				"ga", "gb", "gd", "ge", "gf", "gg", "gh", "gi", "g", "gm",
				"gn", "gp", "gq", "gr", "gs", "gt", "gu", "gw", "gy", "hk",
				"hm", "hn", "hr", "ht", "hu", "id", "ie", "i", "im", "in",
				"io", "iq", "ir", "is", "it", "je", "jm", "jo", "jp", "ke",
				"kg", "kh", "ki", "km", "kn", "kp", "kr", "kw", "ky", "kz",
				"la", "lb", "lc", "li", "lk", "lr", "ls", "lt", "lu", "lv",
				"ly", "ma", "mc", "md", "me", "mg", "mh", "mk", "m", "mm",
				"mn", "mo", "mp", "mq", "mr", "ms", "mt", "mu", "mv", "mw",
				"mx", "my", "mz", "na", "nc", "ne", "nf", "ng", "ni", "n",
				"no", "np", "nr", "nu", "nz", "om", "pa", "pe", "pf", "pg",
				"ph", "pk", "p", "pm", "pn", "pr", "ps", "pt", "pw", "py",
				"qa", "re", "ro", "rs", "ru", "rw", "sa", "sb", "sc", "sd",
				"se", "sg", "sh", "si", "sj", "sk", "s", "sm", "sn", "so",
				"sr", "st", "su", "sv", "sy", "sz", "tc", "td", "tf", "tg",
				"th", "tj", "tk", "t", "tm", "tn", "to", "tp", "tr", "tt",
				"tv", "tw", "tz", "ua", "ug", "uk", "us", "uy", "uz", "va",
				"vc", "ve", "vg", "vi", "vn", "vu", "wf", "ws", "ye", "yt",
				"yu", "za", "zm", "zw", "nt"));

		NavigationURLs = new String[] { "http://abc.ie.sogou.com/",
				"http://123.ie.sogou.com/", "http://123.sogou.com/",
				"http://www.hao123.com/", "http://www.qq5.com/",
				"http://www.go2000.cn/", "http://hao.360.cn/",
				"http://www.114la.com/", "http://www.3355.net/",
				"http://www.6655.com/", "http://www.2345.com/"};
		
		Shopping = new HashSet<String>(Arrays.asList(".dangdang.com",".taobao.com",
				".360buy.com",".zol.com.cn",".tmail.com"));
		
		Video = new HashSet<String>(Arrays.asList(".youku.com",".tudou.com",
				"tv.sohu.com","v.qq.com","www.iqiyi.com","www.tvmao.com",
				"tv.sogou.com",".xunlei.com",".pptv.",".pps.","video.sina.com.cn",
				"letv.com"));
	}

	public static String urlToHost(String url) {
		if (url == null) {
			return "";
		}
		int start = url.indexOf("://");
		if (start != -1) {
			int end = url.indexOf('/', start + 3);
			if (end != -1) {
				return url.substring(start + 3, end);
			} else {
				return url.substring(start + 3);
			}
		}
		return "";
	}

	public static boolean isNavigationPage(String url) {
		if (url != null && !url.equals("")) {
			for (String s : NavigationURLs) {
				if (url.startsWith(s)) {
					return true;
				}
			}
		}
		return false;
	}

	public static boolean isSearchResultPage(String url) {
		if (url != null && !url.equals("")) {
			return (url.startsWith("http://www.sogou.com/web?")
					|| url.startsWith("http://www.sogou.com/sogou?")
					|| url.startsWith("http://www.baidu.com/s?")
					|| url.startsWith("http://www.google.com/search?")
					|| url.startsWith("http://www.google.com.hk/search?")
					|| url.startsWith("http://www.google.cn/search?") || url
					.startsWith("http://www.soso.com/q?"));
		}
		return false;
	}

	public static String findQueryValue(String input, String query) {
		int pos1 = 0;
		int pos2;
		if (input.startsWith(query + '=')) {
			pos1 = query.length() + 1;
		} else {
			pos1 = input.indexOf("&" + query + "=");
			if (pos1 == -1) {
				return "";
			}
			pos1 += query.length() + 2;
		}

		pos2 = input.indexOf('&', pos1);
		if (pos2 == -1) {
			pos2 = input.length();
		}
		if (pos2 > pos1) {
			return input.substring(pos1, pos2);
		} else {
			return "";
		}
	}

	public static String removeSharp(String url) {
		int p = url.indexOf('#');
		if (p != -1) {
			return url.substring(0, p);
		}
		return url;
	}

	public static String urlToSite(String url) {
		String host = urlToHost(url);
		int p = host.indexOf(":");
		if (p != -1) {
			return host.substring(0, p);
		} else {
			return host;
		}
	}

	public static String urlToTopDomain(String url) {
		String site = urlToSite(url);
		if (site != null) {
			return siteToTopDomain(site);
		}
		return "";
	}

	public static String siteToTopDomain(String site) {
		if (site == null) {
			return "";
		}
		String tokens[] = StringMisc.split(site, '.');
		int len = tokens.length;
		if (len == 0) {
			System.err.println(site + " has not split");
		}
		if (len <= 1) {
			return site;
		}
		if (CountryDomain.contains(tokens[len - 1])) {
			if (len > 2 && TopLevelDomain.contains(tokens[len - 2])) {
				return tokens[len - 3];
			} else {
				return tokens[len - 2];
			}
		} else if (TopLevelDomain.contains(tokens[len - 1])) {
			return tokens[len - 2];
		} else {
			return tokens[len - 1];
		}
	}

	public static String siteToDomain(String site) {
		if (site == null) {
			return "";
		}
		String tokens[] = StringMisc.split(site, '.');
		int len = tokens.length;
		if (len == 1) {
			return site;
		}
		if (CountryDomain.contains(tokens[len - 1])) {
			if (len > 2 && TopLevelDomain.contains(tokens[len - 2])) {
				return tokens[len - 3] + "." + tokens[len - 2] + "."
						+ tokens[len - 1];
			} else {
				return tokens[len - 2] + "." + tokens[len - 1];
			}
		} else if (TopLevelDomain.contains(tokens[len - 1])) {
			return tokens[len - 2] + "." + tokens[len - 1];
		} else {
			return tokens[len - 1];
		}
	}

	/**
	 * 去掉www前缀
	 * 
	 * @param site
	 * @return
	 */
	public static String removeWWWPrefix(String site) {
		if (!site.startsWith("www")) {
			return site;
		}
		int pos = site.indexOf('.');
		if (pos == -1) {
			return site;
		} else {
			return site.substring(pos + 1);
		}
	}

	public static boolean isValidSite(String site) {
		String tokens[] = StringMisc.split(site, '.');
		int len = tokens.length;
		if (len >= 1 && CountryDomain.contains(tokens[len - 1])) {
			if (len >= 2 && TopLevelDomain.contains(tokens[len - 2])) {
				return true;
			} else {
				return true;
			}
		} else if (len >= 1 && TopLevelDomain.contains(tokens[len - 1])) {
			return true;
		}
		return false;
	}

	/**
	 * 去掉国际、顶级域名后缀
	 * 
	 * @param site
	 * @return
	 */
	public static String removeSiteSubfix(String site) {
		String tokens[] = StringMisc.split(site, '.');
		int len = tokens.length;
		int stop = len;
		if (len <= 1) {
			return site;
		}
		if (CountryDomain.contains(tokens[len - 1])) {
			if (len > 2 && TopLevelDomain.contains(tokens[len - 2])) {
				stop = len - 2;
			} else {
				stop = len - 1;
			}
		} else if (TopLevelDomain.contains(tokens[len - 1])) {
			stop = len - 1;
		}
		return stop > 0 ? StringMisc.join(Arrays.copyOfRange(tokens, 0, stop),
				'.') : "";
	}

	public static String tidyURL(String url) {
		url = url.replace("\t", "%09");
		url = url.replace("\n", "%20");
		url = url.replace("\r", "%20");
		return url;
	}

	public static String extractSearchWord(String url, String domain) {
		if (domain == null || !SearchEngineDomain.contains(domain)) {
			return null;
		}
		String[] qstrs = SearchEngineQuery.get(domain);
		if (qstrs == null) {
			return null;
		}
		for (String qstr : qstrs) {
			int pos1 = url.indexOf('?' + qstr);
			if (pos1 == -1) {
				pos1 = url.indexOf('&' + qstr);
			}
			if (pos1 == -1) {
				continue;
			}
			int pos2 = url.indexOf('&', pos1 + qstr.length() + 1);
			if (pos2 == -1) {
				pos2 = url.length();
			}
			String raw = url.substring(pos1 + qstr.length() + 1, pos2);
			try {
				return EncodingConv.smartDecode(raw);
			} catch (Exception e) {
				return raw;
			}
		}
		return null;
	}

	public static String extractSearchWord(String url) {
		String site = urlToSite(url);
		if (site.length() == 0) {
			return null;
		}

		String domain = siteToTopDomain(site);
		if (domain.length() == 0) {
			return null;
		}
		return extractSearchWord(url, domain);
	}

//	public static void main(String[] args) throws Exception {
//
//		String inputf = "test.url.in";
//		if (args.length >= 1) {
//			inputf = args[0];
//		}
//
//		BufferedReader input;
//		try {
//			input = new BufferedReader(new FileReader(inputf));
//		} catch (FileNotFoundException e) {
//			System.err.println(inputf + " not found!");
//			return;
//		}
//
//		try {
//			while (input.ready()) {
//				String line = input.readLine();
//				String result = extractSearchWord(line);
//				if (result != null) {
//					System.out.println(result + '\t' + line);
//				}
//			}
//		} catch (IOException e) {
//			;
//		}
//	}

	public static int getUrlWeight(String url) {
		int w = 0;
		boolean qst = false;
		for (int i = 0; i < url.length(); i++) {
			char c = url.charAt(i);
			if (c == '&') {
				w += 2;
			} else if (!qst) {
				if (c == '?') {
					qst = true;
					w += 2;
				} else if (c == '/') {
					w += 2;
				}
			}
		}
		if (url.charAt(url.length() - 1) != '/') {
			w += 1;
		}
		return w;
	}

	public static boolean isSiteRoot(String url) {
		if (url == null) {
			return false;
		}
		int pos = url.indexOf("://");
		if (pos == -1) {
			return false;
		}
		int pos2 = url.indexOf('/', pos + 3);
		if (pos2 == -1 || pos2 == url.length() - 1) {
			return true;
		}
		return false;
	}

	public static boolean isDirRoot(String url) {
		int pos = url.lastIndexOf('.');
		int pos2 = url.lastIndexOf('/');
		if (pos2 == url.length() - 1) {
			return true;
		}
		if (pos > pos2) {
			if (url.substring(pos2 + 1, pos).equalsIgnoreCase("index")) {
				return true;
			}
		}
		return false;
	}

	public static String findURLUpwards(Set<String> set, String url) {
		if (url == null || url.length() == 0) {
			return null;
		}
		// test directly
		if (set.contains(url)) {
			return url;
		}
		// test directory root
		String dirRoot = getDirRoot(url);
		while (dirRoot != null) {
			if (set.contains(dirRoot)) {
				return dirRoot;
			}
			dirRoot = getDirRoot(dirRoot);
		}
		// test www+site
		int pos = url.indexOf("://");
		if (pos <= 0) {
			return null;
		}
		String proto = url.substring(0, pos);
		String site = urlToSite(url);
		if (site == null || site.length() == 0) {
			return null;
		}
		if (!site.startsWith("www.")) {
			String up = proto + "://www." + site + '/';
			if (set.contains(up)) {
				return up;
			}
		}

		// test upper level domains
		for (String upper : getUpperLevelDomains(site)) {
			String up = proto + "://" + upper + '/';
			if (set.contains(up)) {
				return up;
			}
			up = proto + "://www." + upper + '/';
			if (set.contains(up)) {
				return up;
			}
		}
		return null;
	}

	public static <OBJ> OBJ findURLObjectUpwards(Map<String, OBJ> map,
			String url) {
		String up = findURLUpwards(map.keySet(), url);
		if (up != null) {
			return map.get(up);
		}
		return null;
	}

	public static String getDirRoot(String url) {
		if (url == null || url.length() == 0) {
			return null;
		}
		int lp = url.length() - 1;
		while (url.charAt(lp) == '/') {
			lp--;
		}
		int pos = url.lastIndexOf('/', lp);
		if (pos > 8) {
			return url.substring(0, pos + 1);
		}
		return null;
	}

	public static boolean isIPv4(String site) {
		String[] p = site.split("\\.");
		if (p.length != 4) {
			return false;
		}
		for (String s : p) {
			try {
				for (int i = 0; i < s.length(); i++) {
					char ch = s.charAt(i);
					if (ch < '0' || ch > '9') {
						return false;
					}
				}
			} catch (Exception e) {
				return false;
			}
		}
		return true;
	}

	public static String removeIndexPage(String url) {
		final String indexes[] = new String[] { "index", "main", "home" };
		int pos = url.lastIndexOf('/');
		if (pos < 10 || pos == url.length() - 1) {
			return url;
		}
		String fnpart = url.substring(pos + 1);
		boolean isIndex = false;
		for (String s : indexes) {
			if (fnpart.equalsIgnoreCase(s)) {
				isIndex = true;
				break;
			} else if (fnpart.length() > s.length() + 1
					&& fnpart.substring(0, s.length() + 1).equalsIgnoreCase(
							s + '.') && fnpart.indexOf('?') == -1) {
				isIndex = true;
				break;

			}
		}
		if (isIndex) {
			return url.substring(0, pos + 1);
		} else {
			return url;
		}
	}

	/**
	 * 获取站点的上级站点数组，不包含site本身
	 * 
	 * a.b.c.d.com.cn 将得到：b.c.d.com.cn/c.d.com.cn/d.com.cn
	 * 
	 * @param site
	 * @return
	 */
	public static ArrayList<String> getUpperLevelDomains(String site) {
		return getUpperLevelDomains(site, false);
	}

	/**
	 * 获取站点的上级站点数组
	 * 
	 * a.b.c.d.com.cn 将得到：b.c.d.com.cn/c.d.com.cn/d.com.cn
	 * 
	 * @param site
	 * @param includeSelf
	 *            是否包括站点本身
	 * @return
	 */
	public static ArrayList<String> getUpperLevelDomains(String site,
			boolean includeSelf) {
		ArrayList<String> r = new ArrayList<String>();
		if (includeSelf) {
			r.add(site);
		}
		String tokens[] = StringMisc.split(site, '.');
		int len = tokens.length;
		int stop = len;
		if (len <= 1) {
			return r;
		}
		if (CountryDomain.contains(tokens[len - 1])) {
			if (len > 2 && TopLevelDomain.contains(tokens[len - 2])) {
				stop = len - 2;
			} else {
				stop = len - 1;
			}
		} else if (TopLevelDomain.contains(tokens[len - 1])) {
			stop = len - 1;
		}
		for (int j = 1; j < stop; j++) {
			r.add(StringMisc.join(tokens, j, len, '.'));
		}
		return r;
	}
	
	/**
	 * url中是否包含日期
	 * @param url
	 * @return
	 */
	public static boolean IsContainDate(String url){
		//日期类型包括 2012/09/03,2012-09-03
		String regex1 = "http://([\\w|.|-|_]+)[/]{0,1}" +
				"[2]\\d{3}[-,/]{0,1}" +
				"[0-1]\\d[-,/]{0,1}" +
				"[0-3]\\d[/](.*)";
		String regex2 = "http://([\\w|.|-|_]+)[-,/]{1}" +
				"[2]\\d{3}[-,/]{1}" +
				"[0-9]{1}[-,/]{1}" +
				"[0-9]{1}/(.*)";
		Pattern pattern = Pattern.compile(regex1);
		Matcher matcher;
		matcher = pattern.matcher(url);
		
		Pattern pattern2 = Pattern.compile(regex2);
		Matcher matcher2 = pattern2.matcher(url);
		
		return matcher.matches()||matcher2.matches();
	}
	/***
	 * 微博类
	 * @param url
	 * @return
	 */
	public static boolean IsWeibo(String url){
		if(url.indexOf("http://t.qq.com/") != -1
				|| url.indexOf("http://weibo.com/") != -1
				|| url.indexOf("http://http://t.163.com/") != -1
				|| url.indexOf("http://t.sohu.com/") != -1)
			return true;
		return false;
	}
	
	/***
	 * 包括网页及其他类搜索（视频音乐）
	 * @param url
	 * @return
	 */
	public static boolean isSearchResultPageAll(String url) {
		if (url != null && !url.equals("")) {
			return (url.startsWith("http://www.sogou.com/web?")
					|| url.startsWith("http://www.sogou.com/sogou?")
					|| url.startsWith("http://www.baidu.com/s?")
					|| url.startsWith("http://www.google.com/search?")
					|| url.startsWith("http://www.google.com.hk/search?")
					|| url.startsWith("http://www.google.cn/search?") 
					|| url.startsWith("http://www.soso.com/q?")
					|| url.startsWith("http://so.ie.sogou.com/s/?")
					|| url.startsWith("http://pic.sogou.com/pics?")
					|| url.startsWith("http://v.sogou.com/v?")
					|| url.startsWith("http://map.sogou.com/#")
					|| url.startsWith("http://mp3.sogou.com/music.so?=")
					|| url.startsWith("http://zhishi.sogou.com/zhishi?=")
					|| url.startsWith("http://news.sogou.com/news?")
					|| url.startsWith("http://tieba.baidu.com/f?")
					|| url.startsWith("http://zhidao.baidu.com/search?")
					|| url.startsWith("http://mp3.baidu.com/m?")
					|| url.startsWith("http://image.baidu.com/i?")
					|| url.startsWith("http://video.baidu.com/v?")
					|| url.startsWith("http://map.baidu.com/?")
					|| url.startsWith("http://zhidao.baidu.com/q?")
					|| url.startsWith("http://news.baidu.com/ns?")
					|| url.startsWith("http://www.soku.com/v?")
					|| url.startsWith("http://map.baidu.com/m?")
					|| url.startsWith("http://mp3.sogou.com/music.so?")
					|| url.startsWith("http://www.soku.com/search_video/q")
					|| url.startsWith("http://search.xunlei.com/search.php?")
					|| url.startsWith("http://bbs.sogou.com/searchIn.do?"));
		}
		return false;
	}
	/***
	 * 是否搜狗的搜索结果页
	 */
	public static boolean isSearchResultPageSogou(String url) {
		if (url != null && !url.equals("")) {
			return (url.startsWith("http://www.sogou.com/web?")
					|| url.startsWith("http://www.sogou.com/sogou?")				
					|| url.startsWith("http://so.ie.sogou.com/s/?")
					|| url.startsWith("http://pic.sogou.com/pics?")
					|| url.startsWith("http://v.sogou.com/v?")
					|| url.startsWith("http://map.sogou.com/#")
					|| url.startsWith("http://mp3.sogou.com/music.so?=")
					|| url.startsWith("http://zhishi.sogou.com/zhishi?=")
					|| url.startsWith("http://news.sogou.com/news?")
					|| url.startsWith("http://mp3.sogou.com/music.so?")
					|| url.startsWith("http://bbs.sogou.com/searchIn.do?"));
		}
		return false;
	}
	/***
	 * 论坛类
	 */
	public static boolean isDiscuz(String url){
		if(url.indexOf("bbs") != -1
				||url.indexOf("forum") != -1
				||url.indexOf("viewthread") != -1
				||url.indexOf("discuz") != -1
				||url.indexOf("thread") != -1
				)
			return true;
		return false;
	}
	
	/***
	 * 博客及各种空间类
	 */
	public static boolean isZoon(String url){
		if(url.indexOf("http://hi.baidu.com/") != -1
				||url.indexOf("blog") != -1
				||url.indexOf("qzone.qq.com") != -1)
			return true;
		return false;
	}
	
	/**
	 * url包含ip
	 */
	
	public static boolean isIp(String url){
		String site = URLMisc.urlToSite(url);
		Pattern pattern = Pattern.compile("\\b((?!\\d\\d\\d)\\d+|1\\d\\d|2[0-4]\\d|25[0-5])\\." +
				"((?!\\d\\d\\d)\\d+|1\\d\\d|2[0-4]\\d|25[0-5])\\." +
				"((?!\\d\\d\\d)\\d+|1\\d\\d|2[0-4]\\d|25[0-5])\\." +
				"((?!\\d\\d\\d)\\d+|1\\d\\d|2[0-4]\\d|25[0-5])\\b");
		Matcher matcher = pattern.matcher(site); 
		return matcher.matches();
	}
	
	/***
	 * 注册入口页
	 */
	public static boolean IsLogin(String url){
		if(url.indexOf("register.php") != -1
				|| url.indexOf("login") != -1)
			return true;
		return false;
	}
	
	/**
	 * 文库类
	 */
	public static boolean isDoc(String url){
		if(url.startsWith("http://wenku.baidu.com/") 
				|| url.startsWith("http://www.docin.com/")
				|| url.startsWith("http://ishare.iask.sina.com.cn/"))
			return true;
		return false;
	}
	
	/**
	 * 知识类
	 */
	public static boolean isBaike(String url){
		if(url.startsWith("http://zhidao.baidu.com")
				||url.startsWith("http://baike.baidu.com/")
				||url.startsWith("http://zhishi.sogou.com/")
				||url.startsWith("http://www.hudong.com/")
				||url.startsWith("http://baike.soso.com/")
				||url.startsWith("http://zh.wikipedia.org/"))
			return true;
		return false;
	}
	
	/**
	 * 购物类
	 */
	public static boolean isShop(String url){
		if (url != null && !url.equals("")) {
			for (String s : Shopping) {
				if (url.indexOf(s)!= -1) {
					return true;
				}
			}
		}
		return false;
	}
	
	/***
	 * 视频类
	 */
	public static boolean isVideo(String url){
		if (url != null && !url.equals("")) {
			for (String s : Video) {
				if (url.indexOf(s)!= -1) {
					return true;
				}
			}
		}
		return false;
	}
	
	
	/***
	 * 新闻类
	 */
	public static boolean isNews(String url){
		if(url.indexOf("news") != -1)
			return true;
		return false;
	}
	
	/***
	 * url包含中文
	 */
	public static boolean ContainChinese(String url){
		  String regEx = "[\\u4e00-\\u9fa5]";
		  Pattern p = Pattern.compile(regEx);
		  Matcher matcher = p.matcher(url);
		  int count = 0;
		  while(matcher.find()){
			  for (int i = 0; i <= matcher.groupCount(); i++) {
				  count = count + 1;
				  }
			  }
		  if(count > 0) return true;
		  return false;
	}
	public static boolean isbad(String url){
		if(url.indexOf("http://finance.sina.com.cn/") != -1||
				url.indexOf(".duowan.") != -1||
				url.indexOf("http://lady.163.com/") != -1||
				url.indexOf("ent.163") != -1||
				url.indexOf("http://read.shulu.net/") != -1||
				url.indexOf(".douban.com")!= -1||
				url.indexOf(".5ding.com") != -1||
				url.indexOf("ent.qq.com") != -1||
				url.indexOf(".tianya.")!= -1||
				url.indexOf("http://sh.sina.com.cn/")!= -1||
				url.indexOf("http://www.ishuo.cn/")!= -1||
				url.indexOf("http://top.sogou.com/")!= -1||
				url.indexOf("http://bbs.dospy.com/")!= -1||
				url.indexOf(".yy.com")!= -1||
				url.indexOf(".gov.cn") != -1)
			return true;
		return false;
			
	}
	/**
	 * 生活综合类网站
	 */
	public static boolean IslifeIndex(){
		return false;
	}
	public static void main(String[] args){
		String site = "http://news.baidu.com/ns?cl=2&rn=20&tn=news&word=%E6%B9%96%E5%8D%97%E5%8D%AB%E8%A7%86&ie=utf-8";
		System.out.println(isValidSite(site));
		System.out.println(siteToDomain(site));
	}
}
