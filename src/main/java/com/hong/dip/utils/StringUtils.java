package com.hong.dip.utils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author xuhb
 *
 */
public final class StringUtils {
    public static final Map<String, Pattern> PATTERN_MAP = new HashMap<>();
    static {
        String patterns[] = {"/", " ", ":", ",", ";", "=", "\\.", "\\+"};
        for (String p : patterns) {
            PATTERN_MAP.put(p, Pattern.compile(p));
        }
    }

    private StringUtils() {
    }

    public static String[] split(String s, String regex) {
        Pattern p = PATTERN_MAP.get(regex);
        if (p != null) {
            return p.split(s);
        }
        return s.split(regex);
    }
    public static String[] split(String s, String regex, int limit) {
        Pattern p = PATTERN_MAP.get(regex);
        if (p != null) {
            return p.split(s, limit);
        }
        return s.split(regex, limit);
    }

    public static boolean isFileExist(String file) {
        return new File(file).exists() && new File(file).isFile();
    }

    public static boolean isEmpty(String str) {
        if (str != null) {
            int len = str.length();
            for (int x = 0; x < len; ++x) {
                if (str.charAt(x) > ' ') {
                    return false;
                }
            }
        }
        return true;
    }

    public static boolean isEmpty(List<String> list) {
        if (list == null || list.size() == 0) {
            return true;
        }
        return list.size() == 1 && isEmpty(list.get(0));
    }

    public static String diff(String str1, String str2) {
        int index = str1.lastIndexOf(str2);
        if (index > -1) {
            return str1.substring(str2.length());
        }
        return str1;
    }

    public static List<String> getParts(String str, String separator) {
        String[] parts = split(str, separator);
        List<String> ret = new ArrayList<>(parts.length);
        for (String part : parts) {
            if (!isEmpty(part)) {
                ret.add(part);
            }
        }
        return ret;
    }

    public static String getFirstNotEmpty(String str, String separator) {
        List<String> parts = Arrays.asList(split(str, separator));
        for (String part : parts) {
            if (!isEmpty(part)) {
                return part;
            }
        }
        return str;
    }

    public static String getFirstNotEmpty(List<String> list) {
        if (isEmpty(list)) {
            return null;
        }
        for (String item : list) {
            if (!isEmpty(item)) {
                return item;
            }
        }
        return null;
    }

    public static List<String> getFound(String contents, String regex) {
        if (isEmpty(regex) || isEmpty(contents)) {
            return null;
        }
        List<String> results = new ArrayList<>();
        Pattern pattern = Pattern.compile(regex, Pattern.UNICODE_CASE);
        Matcher matcher = pattern.matcher(contents);

        while (matcher.find()) {
            if (matcher.groupCount() > 0) {
                results.add(matcher.group(1));
            } else {
                results.add(matcher.group());
            }
        }
        return results;
    }

    public static String getFirstFound(String contents, String regex) {
        List<String> founds = getFound(contents, regex);
        if (isEmpty(founds)) {
            return null;
        }
        return founds.get(0);
    }

    public static String addDefaultPortIfMissing(String urlString) {
        return addDefaultPortIfMissing(urlString, "80");
    }

    public static String addDefaultPortIfMissing(String urlString, String defaultPort) {
        URL url = null;
        try {
            url = new URL(urlString);
        } catch (MalformedURLException e) {
            return urlString;
        }
        if (url.getPort() != -1) {
            return urlString;
        }
        String regex = "http://([^/]+)";
        String found = StringUtils.getFirstFound(urlString, regex);
        String replacer = "http://" + found + ":" + defaultPort;

        if (!StringUtils.isEmpty(found)) {
            urlString = urlString.replaceFirst(regex, replacer);
        }
        return urlString;
    }

    /**
     * Return input string with first character in upper case.
     * @param name input string.
     * @return capitalized form.
     */
    public static String capitalize(String name) {
        if (name == null || name.length() == 0) {
            return name;
        }
        char chars[] = name.toCharArray();
        chars[0] = Character.toUpperCase(chars[0]);
        return new String(chars);
    }

    public static String uncapitalize(String str) {
        if (str == null || str.length() == 0) {
            return str;
        }
        return new StringBuilder(str.length())
            .append(Character.toLowerCase(str.charAt(0)))
            .append(str.substring(1))
            .toString();
    }

    public static byte[] toBytesUTF8(String str) {
        return toBytes(str, StandardCharsets.UTF_8.name());
    }
    public static byte[] toBytesASCII(String str) {
        return toBytes(str, "US-ASCII");
    }
    public static byte[] toBytes(String str, String enc) {
        try {
            return str.getBytes(enc);
        } catch (UnsupportedEncodingException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static String toHexString(byte[] bytes) {
        StringBuilder hexString = new StringBuilder();
        for (int i = 0; i < bytes.length; i++) {
            hexString.append(Integer.toHexString(0xFF & bytes[i]));
        }
        return hexString.toString();
    }
    
    public static String getContextName(String path) {
        String contextName = "";
        int idx = path.lastIndexOf('/');
        if (idx >= 0) {
            contextName = path.substring(0, idx);
        }
        return contextName;
    }

    public static String getResourceBase(String path) {
        String servletMap = "";
        int idx = path.lastIndexOf('/');
        if (idx >= 0) {
            servletMap = path.substring(idx+1);
        }
        if ("".equals(servletMap) || "".equals(path)) {
            servletMap = "";
        }
        return servletMap;
    }


	public static String list2String(List<String> list) {
		StringBuilder b = new StringBuilder();
		for(String s : list)
			b.append(s).append(';');
		return b.toString();
	}
	public static List<String> string2List(String s) {
		List<String> list = new ArrayList<String>();
		if(s == null)
			return list;
		int start = 0;
		int occurs = -1;
		
		while(start < s.length()){
			occurs = s.indexOf(';', start);
			String part = null;
			if(occurs - start >  0){
				part = s.substring(start, occurs);
				
			}else if(occurs == -1){
				if(start < s.length()){
					part = s.substring(start);
				}
			}
			if(!StringUtils.isEmpty(part)){
				list.add(part);
			}
			//continue next part ';'
			start = occurs < 0 ? s.length() : occurs + 1;
		}
		return list;
	}
	public static void ensureFileExists(File file) throws IOException{
		if(file.exists())
			return;
		else
			mkEmptyFile(file);
	}
	public static void ensureDirExists(File dir) throws IOException{
		if(dir.exists()){
			if(dir.isDirectory())
				return;
			else
				throw new IOException(dir + " exists, but it not a directory");
		}else{
			if(dir.mkdirs())
				return;
			else
				throw new IOException("Cannot create directory (" + dir + ")");
		}
	}

	public static String map2String(Map<String, Integer> names){
		StringBuilder builder = new StringBuilder();
		for(Map.Entry<String, Integer> set : names.entrySet()){
			builder.append(set.getKey()).append(':').append(set.getValue()).append(';');
		}
		return builder.toString();
	}

//	public static Map<String, Integer> parseMapping(String s) {
//		Map<String, Integer> names = new HashMap<String, Integer>();
//		int start = 0;
//		int occurs = -1;
//		
//		while(start < s.length()){
//			occurs = s.indexOf(';', start);
//			String part = null;
//			if(occurs - start >  0){
//				part = s.substring(start, occurs);
//				
//			}else if(occurs == -1){
//				if(start < s.length()){
//					part = s.substring(start);
//				}
//			}
//			if(!MsgTransportCmd.splitNameValue(part, names)){
//				break;
//			}
//			//continue next part ';'
//			start = occurs + 1;
//			if(occurs < 0)
//				break;
//		}
//		if(names.size() == 0)
//			return null;
//		return names;
//	}
    public static class ByteBufferInputStream extends InputStream {
        private final ByteBuffer bb;

        public ByteBufferInputStream(ByteBuffer bb) { 
        	this.bb = bb; 
        }

        @Override public int available() { 
        	return bb.remaining(); 
        }

        @Override public int read() throws IOException {
            if (!bb.hasRemaining()) return -1;
            return bb.get() & 0xFF; // Make sure the value is in [0..255]
        }

        @Override public int read(byte[] bytes, int off, int len) throws IOException {
            if (!bb.hasRemaining()) return -1;
            len = Math.min(len, bb.remaining());
            bb.get(bytes, off, len);
            return len;
        }
    }

	public static byte[] readInputStreamContent(InputStream is) throws IOException{
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		byte[] b = new byte[100];
		int len;
		while((len = is.read(b)) != -1){
			os.write(b, 0, len);
		}
		return os.toByteArray();
	}

	public static void mkEmptyFile(File file) throws IOException{
		FileOutputStream fos = new FileOutputStream(file);
		fos.close();
	}

	public static void writeString(String s, byte[] dest, int offset, int size) {
		byte[] b = s.getBytes();
		size = Math.min(size, b.length);
		System.arraycopy(b, 0, dest, offset, size);
	}

	public static void writeInt(int v, byte[] b, int offset) {
		b[offset] = (byte)(((v & 0xFF000000) >> 24) & 0xFF); 
		b[offset+1] = (byte)(((v & 0xFF0000) >> 16) & 0xFF); 
		b[offset+2] = (byte)(((v & 0xFF00) >> 8) & 0xFF); 
		b[offset+3] = (byte)(v & 0xFF); 
	}
	public static int readInt(byte[] b, int offset) {
		return ((b[offset] & 0xFF) << 24) | 
			((b[offset+1] & 0xFF) << 16) |
			((b[offset+2] & 0xFF) << 8) |
			((b[offset+3] & 0xFF));
	}

	public static String readString(byte[] b, int offset, int size) {
		int len = 0;
		for(int i = offset; i < offset+size; i++){
			if(b[i] == 0)
				break;
			len++;
		}
		return new String(b, offset, len);
	}

	public static boolean strEquals(String s1, String s2) {
		if(s1 == null)
			return s1 == s2;
		else
			return s1.equals(s2);
	}

	public static FileOutputStream openFileOutputStream(File attachment, long offset) {
		
		return null;
	}

	public static boolean byteArrayEquals(byte[] bytes1, byte[] bytes2) {
		if(bytes1 == null){
			return bytes2 == null;
		}else{
			if(bytes2 == null)
				return false;
			if(bytes1.length == bytes2.length){
				for(int i = 0; i < bytes1.length; i++)
					if(bytes1[i] != bytes2[i])
						return false;
				return true;
			}else
				return false;
		}
	}

	/**
	 * create file
	 * @param file file to create
	 * @param size	created file size;
	 * @param c		fill with character
	 */
	public static void touchFile(File file, int size, char c)throws IOException {
		FileOutputStream fos = new FileOutputStream(file);
		try{
			for(int i = 0; i < size; i++){
				fos.write(c);
			}
		}finally{
			fos.getFD().sync();
			fos.close();
		}
	}

}