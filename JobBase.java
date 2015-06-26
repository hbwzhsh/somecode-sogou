package mypackage;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;


/**
 * @author ATP this class is used to define several common functions for
 *         Map-Reduce Job classes
 * 
 */

public abstract class JobBase {

	public Configuration getConfiguration() {
		return conf;
	}

	/**
	 * Print option help to standard output and exit with code 0 including
	 * hadoop generic options
	 * 
	 * @param opt
	 *            the options for current program
	 */
	public void printUsage(Options opt) {
		printUsage(opt, true);
	}

	/**
	 * Print option help to standard output and exit with code 0, can specify
	 * whether to include hadoop generic options
	 * 
	 * @param opt
	 * @param incHadoop
	 */
	public void printUsage(Options opt, boolean incHadoop) {
		printUsage(getClass().getName(), opt, System.out, 0, incHadoop);
	}

	/**
	 * Print option help, including hadoop generic options
	 * 
	 * @param opt
	 *            the options for current program
	 * @param ps
	 *            where to print
	 * @param exitCode
	 *            exit code
	 */
	public void printUsage(Options opt, PrintStream ps, int exitCode) {
		printUsage(getClass().getName(), opt, ps, exitCode, true);
	}

	public abstract boolean run(String[] args) throws Exception;

	public void setConfiguration(Configuration configuration) {
		conf = configuration;
	}

	/**
	 * @param conf
	 * @param files
	 * @return
	 * @throws IOException
	 */
	public static String convJobFileString(Configuration conf, String files)
			throws IOException {
		if (files == null) {
			return null;
		}
		String[] fileArr = files.split(",");
		String[] finalArr = new String[fileArr.length];
		ArrayList<String> fileList = new ArrayList<String>();
		for (int i = 0; i < fileArr.length; i++) {
			String tmp = fileArr[i];
			String finalPath;
			Path path = new Path(tmp);
			URI pathURI = path.toUri();
			FileSystem localFs = FileSystem.getLocal(conf);
			if (pathURI.getScheme() == null) {
				// default to the local file system
				// check if the file exists or not first
				if (!localFs.exists(path)) {
					System.err.println("File " + tmp + " does not exist.");
					finalPath = null;
				} else {
					finalPath = path.makeQualified(localFs).toString();
				}
			} else {
				// check if the file exists in this file system
				// we need to recreate this filesystem object to copy
				// these files to the file system jobtracker is running
				// on.
				FileSystem fs = path.getFileSystem(conf);
				if (!fs.exists(path)) {
					throw new FileNotFoundException("File " + tmp
							+ " does not exist.");
				}
				finalPath = path.makeQualified(fs).toString();
				try {
					fs.close();
				} catch (IOException e) {
				}
				;
			}
			if (finalPath != null) {
				fileList.add(finalPath);
			}
		}
		finalArr = fileList.toArray(new String[fileList.size()]);
		return StringUtils.arrayToString(finalArr);
	}

	/**
	 * @param files
	 * @return
	 * @throws IOException
	 */
	public static String convJobFileString(String files) throws IOException {
		return convJobFileString(new Configuration(), files);
	}

	/**
	 * print error message and exit
	 * 
	 * @param msg
	 *            error message
	 * @param exitCode
	 *            exit code
	 */
	public static void errorExit(String msg, int exitCode) {
		System.err.println(msg + " type -h/--help to see help");
		System.exit(exitCode);
	}

	/**
	 * @param streamMap
	 * @param tableName
	 * @param filePrefix
	 * @param encoding
	 * @return
	 * @throws UnsupportedEncodingException
	 * @throws FileNotFoundException
	 */
	private static PrintStream getNamedStream(
			HashMap<String, PrintStream> streamMap, String tableName,
			String filePrefix, String encoding)
			throws UnsupportedEncodingException, FileNotFoundException {
		PrintStream ps = streamMap.get(tableName);
		if (ps == null) {
			try {
				ps = new PrintStream(new File(filePrefix + "." + tableName
						+ ".txt"), encoding);
			} catch (FileNotFoundException e) {
				e.printStackTrace(System.err);
				return System.out;
			}
			streamMap.put(tableName, ps);
		}
		return ps;
	}

	/**
	 * Get Sogou Rank path in HDFS with given date
	 * 
	 * @param date
	 *            date for sogou rank
	 * @return the string path of sogou rank
	 */
	public static String getSogouRankPathForDate(String date) {
		return "/root/user/sogourank/" + date.substring(0, 6) + "/" + date;
	}

	/**
	 * 将指定路径的utf-8的HDFS内容输出到一个utf-8编码的文件，而且不去除tableName 但是文件内容会使用URLDecode进行处理
	 * 
	 * @param fs
	 * @param pathHDFS
	 * @param fname
	 * @throws IOException
	 */
	public static void outputToFile(FileSystem fs, String pathHDFS, String fname)
			throws IOException {
		outputToFile(fs, pathHDFS, fname, true);
	}

	/**
	 * 将指定路径的utf-8的HDFS内容输出到一个utf-8编码的文件，而且不去除tableName
	 * 可以选择文件内容是否使用URLDecode进行处理
	 * 
	 * @param fs
	 * @param pathHDFS
	 * @param fname
	 * @param doURLDecode
	 * @throws IOException
	 */
	public static void outputToFile(FileSystem fs, String pathHDFS,
			String fname, boolean doURLDecode) throws IOException {
		outputToFile(fs, pathHDFS, fname, false, "::", "utf-8", doURLDecode);
	}

	/**
	 * 将指定路径的HDFS内容输出到一个utf-8编码的文件，可以选择是否去掉行首的tableName
	 * 
	 * @param fs
	 * @param pathHDFS
	 * @param fname
	 * @param removeTableName
	 * @throws IOException
	 */
	public static void outputToFile(FileSystem fs, String pathHDFS,
			String fname, boolean removeTableName, String keySep)
			throws IOException {
		outputToFile(fs, pathHDFS, fname, removeTableName, keySep, "utf-8",
				true);
	}

	public static void outputToFile(FileSystem fs, String pathHDFS,
			String fname, boolean removeTableName, String keySep,
			boolean doURLDecode) throws IOException {
		outputToFile(fs, pathHDFS, fname, removeTableName, keySep, "utf-8",
				doURLDecode);
	}

	/**
	 * 将指定路径的HDFS内容输出到一个文件， 可以选择是否去掉行首的tableName以及对应的分隔符，可以设置输出文件的编码
	 * 
	 * @param fs
	 * @param pathHDFS
	 * @param fname
	 * @param removeTableName
	 * @param keySep
	 * @param encoding
	 *            输出文件的encoding
	 * @param doURLDecode
	 *            HDFS文件的内容输出时是否需要经过URLDecode
	 * @throws IOException
	 */
	public static void outputToFile(FileSystem fs, String pathHDFS,
			String fname, boolean removeTableName, String keySep,
			String encoding, boolean doURLDecode) throws IOException {
		outputToStream(fs, new Path(pathHDFS), new PrintStream(new File(fname),
				encoding), removeTableName, keySep, "utf-8", doURLDecode);
	}

	/**
	 * 将指定路径的HDFS内容输出到一个文件， 可以选择是否去掉行首的tableName以及对应的分隔符，可以设置HDFS的文件编码以及输出文件的编码
	 * 
	 * @param fs
	 * @param pathHDFS
	 * @param fname
	 * @param removeTableName
	 * @param keySep
	 * @param hdfsEncoding
	 * @param encoding
	 * @param doURLDecode
	 * @throws IOException
	 */
	public static void outputToFile(FileSystem fs, String pathHDFS,
			String fname, boolean removeTableName, String keySep,
			String hdfsEncoding, String encoding, boolean doURLDecode)
			throws IOException {
		outputToStream(fs, new Path(pathHDFS), new PrintStream(new File(fname),
				encoding), removeTableName, keySep, hdfsEncoding, doURLDecode);
	}

	private static void outputToSeparateFile(FileSystem fs, Path path,
			HashMap<String, PrintStream> streamMap, String filePrefix,
			String keySep, String encoding) throws IOException {
		if (path.toString().startsWith("_")) {
			return;
		}
		FileStatus fstatus = fs.getFileStatus(path);
		if (fstatus.isDir()) {
			for (FileStatus one : fs.listStatus(path)) {
				Path p = one.getPath();
				outputToSeparateFile(fs, p, streamMap, filePrefix, keySep,
						encoding);
			}
		} else if (!path.getName().startsWith("part-r")) {
			return;
		} else {
			outputToSeparateStream(fs.open(path), streamMap, filePrefix,
					keySep, encoding);
		}
	}

	/**
	 * 将HDFS指定目录的内容根据tableName输出到独立的文件
	 * 
	 * @param fs
	 * @param pathHDFS
	 * @param filePrefix
	 * @throws IOException
	 */
	public static void outputToSeparateFile(FileSystem fs, String pathHDFS,
			String filePrefix) throws IOException {
		outputToSeparateFile(fs, pathHDFS, filePrefix, "::");
	}

	/**
	 * 将HDFS指定目录的内容根据tableName输出到独立的文件 可以指定tableName分隔符
	 * 
	 * @param fs
	 * @param pathHDFS
	 * @param filePrefix
	 * @param keySep
	 * @throws IOException
	 */
	public static void outputToSeparateFile(FileSystem fs, String pathHDFS,
			String filePrefix, String keySep) throws IOException {
		outputToSeparateFile(fs, pathHDFS, filePrefix, keySep, "utf-8");
	}

	/**
	 * 将HDFS指定目录的内容根据tableName输出到独立的文件 可以指定tableName分隔符和文件输出格式
	 * 
	 * @param fs
	 * @param pathHDFS
	 * @param filePrefix
	 * @param keySep
	 * @param encoding
	 * @throws IOException
	 */
	public static void outputToSeparateFile(FileSystem fs, String pathHDFS,
			String filePrefix, String keySep, String encoding)
			throws IOException {
		outputToSeparateFile(fs, new Path(pathHDFS),
				new HashMap<String, PrintStream>(), filePrefix, keySep,
				encoding);
	}

	/**
	 * @param fin
	 * @param streamMap
	 * @param filePrefix
	 * @param keySep
	 * @param encoding
	 * @throws UnsupportedEncodingException
	 */
	private static void outputToSeparateStream(FSDataInputStream fin,
			HashMap<String, PrintStream> streamMap, String filePrefix,
			String keySep, String encoding) throws UnsupportedEncodingException {
		BufferedReader br = new BufferedReader(new InputStreamReader(fin,
				"utf-8"));

		while (true) {
			String line;
			try {
				line = br.readLine();
				if (line == null) {
					break;
				}
			} catch (IOException e) {
				break;
			}

			try {
				String str = URLDecoder.decode(line, "utf-8");
				String tableName;
				String value;
				int pos = str.indexOf(keySep);
				if (pos == -1) {
					tableName = "unknown";
					value = str;
				} else {
					tableName = str.substring(0, pos);
					value = str.substring(pos + keySep.length());
				}
				getNamedStream(streamMap, tableName, filePrefix, encoding)
						.println(value);
			} catch (Exception e) {
			}
		}
	}

	/**
	 * 递归的将HDFS上指定Path的内容输出到PrintStream中， 可以选择是否去掉tableName以及对应的分隔符
	 * 
	 * @param fs
	 * @param path
	 * @param ps
	 * @param removeTableName
	 * @param keySep
	 * @throws UnsupportedEncodingException
	 * @throws IOException
	 */
	private static void outputToStream(FileSystem fs, Path path,
			PrintStream ps, boolean removeTableName, String keySep,
			String hdfsEnc, boolean doURLDecode)
			throws UnsupportedEncodingException, IOException {
		FileStatus fstatus = fs.getFileStatus(path);
		if (fstatus.isDir()) {
			for (FileStatus one : fs.listStatus(path)) {
				Path p = one.getPath();
				outputToStream(fs, p, ps, removeTableName, keySep, hdfsEnc,
						doURLDecode);
			}
		} else if (!path.getName().startsWith("part-r")) {
			return;
		} else {
			outputToStream(fs.open(path), ps, removeTableName, keySep, hdfsEnc,
					doURLDecode);
		}
	}

	private static void outputToStream(FSDataInputStream fin, PrintStream ps,
			boolean removeTableName, String keySep, String hdfsEnc,
			boolean doURLDecode) throws UnsupportedEncodingException {
		BufferedReader br = new BufferedReader(new InputStreamReader(fin,
				hdfsEnc));
		while (true) {
			String line;
			try {
				line = br.readLine();
				if (line == null) {
					break;
				}
			} catch (IOException e) {
				break;
			}

			try {
				String str = doURLDecode ? URLDecoder.decode(line, "utf-8")
						: line;
				if (removeTableName) {
					int pos = str.indexOf(keySep);
					if (pos != -1) {
						str = str.substring(pos + keySep.length());
					}
				}
				ps.println(str);
			} catch (Exception e) {
			}
		}
	}

	public static void println(String string) {
		println(string, System.out);
	}

	public static void println(String string, PrintStream stream) {
		stream.println(string);
	}

	public static void printUsage(Object obj, Options opt) {
		printUsage(obj, opt, false);
	}

	public static void printUsage(Object obj, Options opt, boolean incHadoop) {
		printUsage(obj.getClass().getName(), opt, System.out, 0, incHadoop);
	}

	public static void printUsage(String name, Options opt) {
		printUsage(name, opt, System.out, 0, false);
	}

	public static void printUsage(String name, Options opt, boolean incHadoop) {
		printUsage(name, opt, System.out, 0, incHadoop);
	}

	public static void printUsage(String name, Options opt, PrintStream ps,
			int exitCode, boolean incHadoop) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp(name + (incHadoop ? " [Generic Option]" : "")
				+ " [Options]", opt, true);
		if (incHadoop) {
			GenericOptionsParser.printGenericCommandUsage(ps);
		}
		System.exit(exitCode);
	}

	public static void run(JobBase job, Configuration conf, String[] args)
			throws Exception {
		if (job != null) {
			job.conf = conf;
			GenericOptionsParser goParser = new GenericOptionsParser(conf, args);
			System.exit(job.run(goParser.getRemainingArgs()) ? 0 : -1);
		}
	}

	/**
	 * @param conf
	 * @param files
	 * @throws IOException
	 */
	public static void setJobFileInConf(Configuration conf, String files)
			throws IOException {
		final String tconf = "tmpfiles";
		String tmpfiles = conf.get(tconf);
		if (tmpfiles == null) {
			conf.set(tconf, convJobFileString(conf, files));
		} else {
			conf.set(tconf, tmpfiles + ',' + convJobFileString(conf, files));
		}
	}

	public static boolean writeReportFile(FileSystem fs, String file,
			String content) {
		Path path = new Path(file);
		try {
			if (fs.exists(path) && fs.getFileStatus(path).isDir()) {
				return false;
			}
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}

		PrintStream stream;
		try {
			stream = new PrintStream(fs.create(path));
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}

		stream.print(content);
		return true;
	}

	protected Configuration conf;
}
