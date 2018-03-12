package priv.marionette.Exception;

import priv.marionette.api.ErrorCode;
import priv.marionette.tools.Constants;
import priv.marionette.tools.SortedProperties;
import priv.marionette.tools.StringUtils;
import priv.marionette.tools.Utils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

/**
 * 数据库操作异常
 *
 * @author Yue Yu
 * @create 2018-01-09 下午4:17
 **/
public class DbException  extends RuntimeException{
    private static final long serialVersionUID = 1L;

    private static final Properties MESSAGES = new Properties();


    static {
        try {
            byte[] messages = Utils.getResource(
                    "/org/h2/res/_messages_en.prop");
            if (messages != null) {
                MESSAGES.load(new ByteArrayInputStream(messages));
            }
            String language = Locale.getDefault().getLanguage();
            if (!"en".equals(language)) {
                byte[] translations = Utils.getResource(
                        "/org/h2/res/_messages_" + language + ".prop");
                // message: translated message + english
                // (otherwise certain applications don't work)
                if (translations != null) {
                    Properties p = SortedProperties.fromLines(
                            new String(translations, Constants.UTF8));
                    for (Map.Entry<Object, Object> e : p.entrySet()) {
                        String key = (String) e.getKey();
                        String translation = (String) e.getValue();
                        if (translation != null && !translation.startsWith("#")) {
                            String original = MESSAGES.getProperty(key);
                            String message = translation + "\n" + original;
                            MESSAGES.put(key, message);
                        }
                    }
                }
            }
        } catch (OutOfMemoryError e) {
            DbException.traceThrowable(e);
        } catch (IOException e) {
            DbException.traceThrowable(e);
        }
    }

    private DbException(SQLException e) {
        super(e.getMessage(), e);
    }

    private static String translate(String key, String... params) {
        String message = null;
        if (MESSAGES != null) {
            // Tomcat sets final static fields to null sometimes
            message = MESSAGES.getProperty(key);
        }
        if (message == null) {
            message = "(Message " + key + " not found)";
        }
        if (params != null) {
            for (int i = 0; i < params.length; i++) {
                String s = params[i];
                if (s != null && s.length() > 0) {
                    params[i] = StringUtils.quoteIdentifier(s);
                }
            }
            message = MessageFormat.format(message, (Object[]) params);
        }
        return message;
    }


    /**
     * Create a database exception for a specific error code.
     *
     * @param errorCode the error code
     * @return the exception
     */
    public static DbException get(int errorCode) {
        return get(errorCode, (String) null);
    }

    /**
     * Create a database exception for a specific error code.
     *
     * @param errorCode the error code
     * @param p1 the first parameter of the message
     * @return the exception
     */
    public static DbException get(int errorCode, String p1) {
        return get(errorCode, new String[] { p1 });
    }

    /**
     * Create a database exception for a specific error code.
     *
     * @param errorCode the error code
     * @param cause the cause of the exception
     * @param params the list of parameters of the message
     * @return the exception
     */
    public static DbException get(int errorCode, Throwable cause,
                                  String... params) {
        return new DbException(getJdbcSQLException(errorCode, cause, params));
    }

    /**
     * Create a database exception for a specific error code.
     *
     * @param errorCode the error code
     * @param params the list of parameters of the message
     * @return the exception
     */
    public static DbException get(int errorCode, String... params) {
        return new DbException(getJdbcSQLException(errorCode, null, params));
    }


    /**
     * Convert a throwable to an SQL exception using the default mapping. All
     * errors except the following are re-thrown: StackOverflowError,
     * LinkageError.
     *
     * @param e the root cause
     * @return the exception object
     */
    public static DbException convert(Throwable e) {
        if (e instanceof DbException) {
            return (DbException) e;
        } else if (e instanceof SQLException) {
            return new DbException((SQLException) e);
        } else if (e instanceof InvocationTargetException) {
            return convertInvocation((InvocationTargetException) e, null);
        } else if (e instanceof IOException) {
            return get(ErrorCode.IO_EXCEPTION_1, e, e.toString());
        } else if (e instanceof OutOfMemoryError) {
            return get(ErrorCode.OUT_OF_MEMORY, e);
        } else if (e instanceof StackOverflowError || e instanceof LinkageError) {
            return get(ErrorCode.GENERAL_ERROR_1, e, e.toString());
        } else if (e instanceof Error) {
            throw (Error) e;
        }
        return get(ErrorCode.GENERAL_ERROR_1, e, e.toString());
    }

    /**
     * Convert an InvocationTarget exception to a database exception.
     *
     * @param te the root cause
     * @param message the added message or null
     * @return the database exception object
     */
    public static DbException convertInvocation(InvocationTargetException te,
                                                String message) {
        Throwable t = te.getTargetException();
        if (t instanceof SQLException || t instanceof DbException) {
            return convert(t);
        }
        message = message == null ? t.getMessage() : message + ": " + t.getMessage();
        return get(ErrorCode.EXCEPTION_IN_FUNCTION_1, t, message);
    }

    /**
     * Convert an IO exception to a database exception.
     *
     * @param e the root cause
     * @param message the message or null
     * @return the database exception object
     */
    public static DbException convertIOException(IOException e, String message) {
        if (message == null) {
            Throwable t = e.getCause();
            if (t instanceof DbException) {
                return (DbException) t;
            }
            return get(ErrorCode.IO_EXCEPTION_1, e, e.toString());
        }
        return get(ErrorCode.IO_EXCEPTION_2, e, e.toString(), message);
    }

    /**
     * Gets the SQL exception object for a specific error code.
     *
     * @param errorCode the error code
     * @param cause the cause of the exception
     * @param params the list of parameters of the message
     * @return the SQLException object
     */
    private static JDBCException getJdbcSQLException(int errorCode,
                                                        Throwable cause, String... params) {
        String sqlstate = ErrorCode.getState(errorCode);
        String message = translate(sqlstate, params);
        return new JDBCException(message, null, sqlstate, errorCode, cause, null);
    }

    /**
     * Convert an exception to an IO exception.
     *
     * @param e the root cause
     * @return the IO exception
     */
    public static IOException convertToIOException(Throwable e) {
        if (e instanceof IOException) {
            return (IOException) e;
        }
        if (e instanceof JDBCException) {
            JDBCException e2 = (JDBCException) e;
            if (e2.getOriginalCause() != null) {
                e = e2.getOriginalCause();
            }
        }
        return new IOException(e.toString(), e);
    }


    /**
     * Write the exception to the driver manager log writer if configured.
     *
     * @param e the exception
     */
    public static void traceThrowable(Throwable e) {
        PrintWriter writer = DriverManager.getLogWriter();
        if (writer != null) {
            e.printStackTrace(writer);
        }
    }

}
