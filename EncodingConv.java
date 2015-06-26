/**
 * 
 */
package mypackage;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author ATP
 * 
 */
public class EncodingConv {

    public static String smartDecode(String str)
        throws UnsupportedEncodingException {
        if (str.indexOf("%u") != -1) {
            str = unicodeToString(str);
        }
        if (str.indexOf('%') != -1) {
            if (guessIfUTF8(str)) {
                str = URLDecoder.decode(str, "utf-8");
            } else {
                str = URLDecoder.decode(str, "gbk");
            }
        }
        return str;
    }

    public static boolean guessIfUTF8(String str) {
        int pos = str.indexOf('%');
        while (pos != -1) {
            char ch = str.charAt(pos + 1);
            if (ch <= '7' && ch >= '0') {
                // this is ASC, skip it
                pos = str.indexOf('%', pos + 1);
                continue;
            }
            if (ch != 'E') {
                // this is not valid Chinese, so return false
                return false;
            }
            // every Chinese char should contain 3 %XX
            pos = str.indexOf('%', pos + 1);
            if (pos == -1) {
                return false;
            }
            pos = str.indexOf('%', pos + 1);
            if (pos == -1) {
                return false;
            }
            pos = str.indexOf('%', pos + 1);
        }
        return true;
    }

    public static String unicodeToString(String str) {
        Pattern pattern = Pattern.compile("(\\%u(\\p{XDigit}{4}))");
        Matcher matcher = pattern.matcher(str);
        char ch;
        while (matcher.find()) {
            ch = (char) Integer.parseInt(matcher.group(2), 16);
            str = str.replace(matcher.group(1), ch + "");
        }
        return str;
    }

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        String encIn = "utf-8";
        String encOut = "gbk";
        if (args.length > 0) {
            encIn = args[0];
        }
        if (args.length > 1) {
            encOut = args[1];
        }

        BufferedReader input;
        PrintStream ps;
        try {
            input = new BufferedReader(new InputStreamReader(System.in, encIn));
            ps = new PrintStream(System.out, true, encOut);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return;
        }
        while (true) {
            try {
                String line = input.readLine();
                if (line == null) {
                    break;
                }
                ps.println(smartDecode(line));
            } catch (IOException e) {
                break;
            }
        }
    }
}
