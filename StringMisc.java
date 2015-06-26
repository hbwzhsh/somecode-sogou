/**
 * 
 */
package mypackage;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;

/**
 * @author ATP (xujun@sogou-inc.com)
 * 
 *         My utilities for string operations
 */
public class StringMisc {

	public enum UTFByteType {
		ASCII, FOLLOW, INVALID, LEAD2BYTES, LEAD3BYTES
	}

	public static int count(String input, char ch) {
		int count = 0;
		for (int i = 0; i < input.length(); i++) {
			if (input.charAt(i) == ch) {
				count++;
			}
		}
		return count;
	}

	public static int count(String input, String target) {
		int count = 0;
		int[] next = getKMPFailure(target);
		int pos = kmpIndexOf(input, target, next, 0);
		while (pos != -1) {
			count++;
			pos = kmpIndexOf(input, target, next, pos + 1);
		}
		return count;
	}

	public static int countIgnoreCase(String input, char ch) {
		int count = 0;
		for (int i = 0; i < input.length(); i++) {
			if (equalIgnoreCase(input.charAt(i), ch)) {
				count++;
			}
		}
		return count;
	}

	public static int countIgnoreCase(String input, String target) {
		int count = 0;
		int[] next = getKMPFailureIgnoreCase(target);
		int pos = kmpIndexOfIgnoreCase(input, target, next, 0);
		while (pos != -1) {
			count++;
			pos = kmpIndexOfIgnoreCase(input, target, next, pos + 1);
		}
		return count;
	}

	public static boolean equalIgnoreCase(char ch1, char ch2) {
		if (ch1 == ch2) {
			return true;
		} else if (Character.toLowerCase(ch1) == Character.toLowerCase(ch2)) {
			return true;
		}
		return false;
	}

	public static UTFByteType getByteType(byte b) {
		if (b >= 0 && b <= 127) {
			return UTFByteType.ASCII;
		} else if (b <= -65 && b >= -128) {
			return UTFByteType.FOLLOW;
		} else if (b <= -17 && b >= -32) {
			return UTFByteType.LEAD3BYTES;
		} else if (b <= -33 && b >= -64) {
			return UTFByteType.LEAD2BYTES;
		} else {
			return UTFByteType.INVALID;
		}
	}

	public static int[] getKMPFailure(String target) {
		int[] next = new int[target.length()];
		int i = 0, j = -1;
		next[0] = -1;
		while (i < target.length() - 1) {
			if (j < 0) {
				next[++i] = ++j;
			} else if (target.charAt(i) == target.charAt(j)) {
				next[i] = -1;
				next[++i] = ++j;
			} else {
				j = next[j];
			}
		}
		if (target.charAt(i) == target.charAt(j)) {
			next[i] = -1;
		}
		return next;
	}

	public static int[] getKMPFailureIgnoreCase(String target) {
		int[] next = new int[target.length()];
		int i = 0, j = -1;
		next[0] = -1;
		while (i < target.length() - 1) {
			if (j < 0) {
				next[++i] = ++j;
			} else if (equalIgnoreCase(target.charAt(i), target.charAt(j))) {
				next[i] = -1;
				next[++i] = ++j;
			} else {
				j = next[j];
			}
		}
		if (equalIgnoreCase(target.charAt(i), target.charAt(j))) {
			next[i] = -1;
		}
		return next;
	}

	public static int indexOfIgnoreCase(String str, char target) {
		return indexOfIgnoreCase(str, target, 0);
	}

	public static int indexOfIgnoreCase(String str, char target, int offset) {
		if (offset >= str.length()) {
			return -1;
		}
		for (int i = offset; i < str.length(); i++) {
			char ch = str.charAt(offset);
			if (equalIgnoreCase(ch, target)) {
				return i;
			}
		}
		return -1;
	}

	public static int indexOfIgnoreCase(String str, String target) {
		return indexOfIgnoreCase(str, target, 0);
	}

	public static int indexOfIgnoreCase(String str, String target, int offset) {
		int max = str.length() - target.length();
		if (max < offset) {
			return -1;
		}
		for (int i = offset; i <= max; i++) {
			if (str.regionMatches(true, i, target, 0, target.length())) {
				return i;
			}
		}
		return -1;
	}

	public static boolean isAllASC(String string) {
		for (int i = 0; i < string.length(); i++) {
			char ch = string.charAt(i);
			if (!isASC(ch)) {
				return false;
			}
		}
		return true;
	}

	public static boolean isAllChinese(String string) {
		for (int i = 0; i < string.length(); i++) {
			char ch = string.charAt(i);
			if (!isChineseChar(ch)) {
				return false;
			}
		}
		return true;
	}

	public static boolean isASC(char ch) {
		return ch <= 0x7f;
	}

	public static boolean isChineseChar(char ch) {
		Character.UnicodeBlock ub = Character.UnicodeBlock.of(ch);
		if (ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS
				|| ub == Character.UnicodeBlock.CJK_COMPATIBILITY_IDEOGRAPHS
				|| ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A
				|| ub == Character.UnicodeBlock.GENERAL_PUNCTUATION
				|| ub == Character.UnicodeBlock.CJK_SYMBOLS_AND_PUNCTUATION
				|| ub == Character.UnicodeBlock.HALFWIDTH_AND_FULLWIDTH_FORMS) {
			return true;
		}
		return false;
	}

	/**
	 * 判断string是否包含非法的UTF-8字符
	 * 
	 * @param string
	 * @return
	 */
	public static boolean isInvalidUTF8String(String string) {
		byte[] bytes;
		try {
			bytes = string.getBytes("utf-8");
		} catch (UnsupportedEncodingException e) {
			System.err.println("UTF-8 not supported!");
			return true;
		}

		for (int i = 0; i < bytes.length; i++) {
			UTFByteType type = getByteType(bytes[i]);
			if (type == UTFByteType.ASCII) {
				continue;
			} else if (type == UTFByteType.LEAD3BYTES) {
				if (i > bytes.length - 3) {
					return true;
				}
				if (getByteType(bytes[i + 1]) != UTFByteType.FOLLOW
						&& getByteType(bytes[i + 2]) != UTFByteType.FOLLOW) {
					return true;
				}
				i += 2;
			} else if (type == UTFByteType.LEAD2BYTES) {
				if (i > bytes.length - 2) {
					return true;
				}
				if (getByteType(bytes[i + 1]) != UTFByteType.FOLLOW) {
					return true;
				}
				i += 1;
			} else {
				return true;
			}
		}
		return false;
	}

	public static boolean isInvalidUTF8StringFast(String string) {
		byte[] bytes;
		try {
			bytes = string.getBytes("utf-8");
		} catch (UnsupportedEncodingException e) {
			System.err.println("UTF-8 not supported!");
			return true;
		}

		for (int i = 0; i < bytes.length; i++) {
			if (isInvalidUTF8Byte(bytes[i])) {
				return true;
			}
		}
		return false;
	}

	public static boolean isInvalidUTF8Byte(byte b) {
		return (b <= -1 && b >= -16);
	}

	public static String join(String[] strs, char seg) {
		return join(strs, 0, strs.length, seg);
	}

	public static String join(String[] strs, int start, int stop, char seg) {
		if (strs == null || strs.length == 0 || start >= stop
				|| stop > strs.length) {
			return "";
		}
		StringBuilder sb = new StringBuilder(strs[start]);
		for (int i = start + 1; i < stop; i++) {
			sb.append(seg);
			sb.append(strs[i]);
		}
		return sb.toString();
	}

	public static String join(String[] strs, int start, int stop, String seg) {
		if (strs == null || strs.length == 0 || start >= stop
				|| stop > strs.length) {
			return "";
		}
		StringBuilder sb = new StringBuilder(strs[start]);
		for (int i = start + 1; i < stop; i++) {
			sb.append(seg);
			sb.append(strs[i]);
		}
		return sb.toString();
	}

	public static String join(String[] strs, String seg) {
		return join(strs, 0, strs.length, seg);
	}

	public static int kmpIndexOf(String str, String target) {
		return kmpIndexOf(str, target, 0);
	}

	public static int kmpIndexOf(String str, String target, int offset) {
		if (str.length() - target.length() < offset) {
			return -1;
		}
		int next[] = getKMPFailure(target);
		return kmpIndexOf(str, target, next, offset);
	}

	public static int kmpIndexOf(String str, String target, int[] next,
			int offset) {
		if (str.length() - target.length() < offset) {
			return -1;
		}
		int i = offset;
		int j = 0;
		while (i < str.length() && j < target.length()) {
			if (j < 0 || str.charAt(i) == target.charAt(j)) {
				i++;
				j++;
			} else {
				j = next[j];
			}
		}
		// System.out.println(i + " " + j);
		if (j >= target.length()) {
			return i - target.length();
		}
		return -1;
	}

	public static int kmpIndexOfIgnoreCase(String str, String target) {
		return kmpIndexOfIgnoreCase(str, target, 0);
	}

	public static int kmpIndexOfIgnoreCase(String str, String target, int offset) {
		if (str.length() - target.length() < offset) {
			return -1;
		}
		int next[] = getKMPFailureIgnoreCase(target);
		int i = offset;
		int j = 0;
		while (i < str.length() && j < target.length()) {
			if (j < 0 || equalIgnoreCase(str.charAt(i), target.charAt(j))) {
				i++;
				j++;
			} else {
				j = next[j];
			}
		}
		// System.out.println(i + " " + j);
		if (j >= target.length()) {
			return i - target.length();
		}
		return -1;
	}

	public static int kmpIndexOfIgnoreCase(String str, String target,
			int[] next, int offset) {
		if (str.length() - target.length() < offset) {
			return -1;
		}
		int i = offset;
		int j = 0;
		while (i < str.length() && j < target.length()) {
			if (j < 0 || equalIgnoreCase(str.charAt(i), target.charAt(j))) {
				i++;
				j++;
			} else {
				j = next[j];
			}
		}
		// System.out.println(i + " " + j);
		if (j >= target.length()) {
			return i - target.length();
		}
		return -1;
	}

	public static int lastIndexOfIgnoreCase(String str, char target) {
		return lastIndexOfIgnoreCase(str, target, str.length() - 1);
	}

	public static int lastIndexOfIgnoreCase(String str, char target, int offset) {
		if (offset >= str.length()) {
			offset = str.length() - 1;
		}
		if (offset < 0) {
			return -1;
		}
		for (int i = offset; i >= 0; i--) {
			char ch = str.charAt(i);
			if (ch == target) {
				return i;
			}
			if (Character.toLowerCase(ch) == Character.toLowerCase(target)) {
				return i;
			}
		}
		return -1;
	}

	public static int lastIndexOfIgnoreCase(String str, String target) {
		return lastIndexOfIgnoreCase(str, target, str.length() - 1);
	}

	public static int lastIndexOfIgnoreCase(String str, String target,
			int offset) {
		int max = str.length() - target.length();
		if (offset > max) {
			offset = max;
		}
		if (offset < 0) {
			return -1;
		}
		for (int i = offset; i >= 0; i--) {
			if (str.regionMatches(true, i, target, 0, target.length())) {
				return i;
			}
		}
		return -1;
	}

	public static void main(String[] args) throws Exception {
		System.out.println("Testing KMP failure mapping");
		int next[] = getKMPFailure(args[0]);
		for (int ii = 0; ii < next.length; ii++) {
			System.out.println(next[ii]);
		}

		System.out.println("Testing KMP searching ignore case");
		int pos = kmpIndexOfIgnoreCase(args[0], args[1]);
		while (pos != -1) {
			System.out.println(pos);
			pos = kmpIndexOfIgnoreCase(args[0], args[1], pos + 1);
		}

		System.out.println("Testing KMP counting");
		System.out.println(countIgnoreCase(args[0], args[1]));
	}

	public static String outputStringCode(String string)
			throws UnsupportedEncodingException {
		return outputStringCode(string, "utf-8");
	}

	public static String outputStringCode(String string, String charset)
			throws UnsupportedEncodingException {
		byte bs[] = string.getBytes(charset);
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < bs.length; i++) {
			sb.append(String
					.format("%d\t%d\t%X\n", i, (int) bs[i], (int) bs[i]));
		}
		return sb.toString();
	}

	public static String[] split(String input, char seg) {
		ArrayList<String> a = new ArrayList<String>();
		int start = 0;
		int len = input.length();
		while (start < len) {
			int pos = input.indexOf(seg, start);
			if (pos == -1) {
				pos = len;
			}
			a.add(input.substring(start, pos));
			start = pos + 1;
		}
		return a.toArray(new String[a.size()]);
	}

	public static String[] split(String input, String seg) {
		ArrayList<String> a = new ArrayList<String>();
		int start = 0;
		int seplen = seg.length();
		int len = input.length();
		while (start < len) {
			int pos = input.indexOf(seg, start);
			if (pos == -1) {
				pos = len;
			}
			a.add(input.substring(start, pos));
			start = pos + seplen;
		}
		return a.toArray(new String[a.size()]);
	}

	public static boolean startsWithIgnoreCase(String str, String prefix) {
		return startsWithIgnoreCase(str, prefix, 0);
	}

	public static boolean startsWithIgnoreCase(String str, String prefix,
			int offset) {
		if (str.length() - offset < prefix.length()) {
			return false;
		}
		return str.regionMatches(true, offset, prefix, 0, prefix.length());
	}

	/**
	 * 按照一定间隔从字符串中截取子串
	 * 
	 * @param input
	 *            源字符串
	 * @param start
	 *            源字符串截取的起始下标
	 * @param end
	 *            源字符串截取的终止下标
	 * @param step
	 *            截取间隔
	 * @return
	 */
	public static String stepSubstring(String input, int start, int end,
			int step) {
		int len = input.length();
		if (start < 0) {
			start = 0;
		}
		if (end > len) {
			end = len;
		}
		if (end <= start || step < 1) {
			throw new StringIndexOutOfBoundsException();
		}
		StringBuilder s = new StringBuilder();
		for (int i = start; i < end; i += step) {
			s.append(input.charAt(i));
		}
		return s.toString();
	}
}
