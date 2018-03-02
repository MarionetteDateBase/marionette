package priv.marionette.tools;

import priv.marionette.shell.FileSystemOperation;

import java.io.*;
import java.util.*;

/**
 * 有序map型配置类
 *
 * @author Yue Yu
 * @create 2018-01-09 下午3:43
 **/
public class SortedProperties extends Properties{

    private static final long serialVersionUID = 1L;

    @Override
    public synchronized Enumeration<Object> keys() {
        Vector<String> v = new Vector<>();
        for (Object o : keySet()) {
            v.add(o.toString());
        }
        Collections.sort(v);
        return new Vector<Object>(v).elements();
    }

    /**
     * Get a boolean property value from a properties object.
     *
     * @param prop the properties object
     * @param key the key
     * @param def the default value
     * @return the value if set, or the default value if not
     */
    public static boolean getBooleanProperty(Properties prop, String key,
                                             boolean def) {
        String value = prop.getProperty(key, "" + def);
        try {
            return Boolean.parseBoolean(value);
        } catch (Exception e) {
            e.printStackTrace();
            return def;
        }
    }

    /**
     * Get an int property value from a properties object.
     *
     * @param prop the properties object
     * @param key the key
     * @param def the default value
     * @return the value if set, or the default value if not
     */
    public static int getIntProperty(Properties prop, String key, int def) {
        String value = prop.getProperty(key, "" + def);
        try {
            return Integer.decode(value);
        } catch (Exception e) {
            e.printStackTrace();
            return def;
        }
    }

    /**
     * Load a properties object from a file.
     *
     * @param fileName the name of the properties file
     * @return the properties object
     */
    public static synchronized SortedProperties loadProperties(String fileName)
            throws IOException {
        SortedProperties prop = new SortedProperties();
        if (FileSystemOperation.exists(fileName)) {
            try (InputStream in = FileSystemOperation.newInputStream(fileName)) {
                prop.load(in);
            }
        }
        return prop;
    }

    /**
     * Store a properties file. The header and the date is not written.
     *
     * @param fileName the target file name
     */
    public synchronized void store(String fileName) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        store(out, null);
        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        InputStreamReader reader = new InputStreamReader(in, "ISO8859-1");
        LineNumberReader r = new LineNumberReader(reader);
        Writer w;
        try {
            w = new OutputStreamWriter(FileSystemOperation.newOutputStream(fileName, false));
        } catch (Exception e) {
            throw new IOException(e.toString(), e);
        }
        PrintWriter writer = new PrintWriter(new BufferedWriter(w));
        while (true) {
            String line = r.readLine();
            if (line == null) {
                break;
            }
            if (!line.startsWith("#")) {
                writer.print(line + "\n");
            }
        }
        writer.close();
    }

    /**
     * Convert the map to a list of line in the form key=value.
     *
     * @return the lines
     */
    public synchronized String toLines() {
        StringBuilder buff = new StringBuilder();
        for (Map.Entry<Object, Object> e : new TreeMap<>(this).entrySet()) {
            buff.append(e.getKey()).append('=').append(e.getValue()).append('\n');
        }
        return buff.toString();
    }

    /**
     * Convert a String to a map.
     *
     * @param s the string
     * @return the map
     */
    public static SortedProperties fromLines(String s) {
        SortedProperties p = new SortedProperties();
        for (String line : StringUtils.arraySplit(s, '\n', true)) {
            int idx = line.indexOf('=');
            if (idx > 0) {
                p.put(line.substring(0, idx), line.substring(idx + 1));
            }
        }
        return p;
    }

}
