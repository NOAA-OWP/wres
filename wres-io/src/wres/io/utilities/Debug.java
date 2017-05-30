package wres.io.utilities;

import java.io.PrintStream;

import org.slf4j.Logger;

import wres.io.config.SystemSettings;
import wres.util.Strings;

/**
 * Provides a process of directing output messaging to either a print stream or to a logger
 * @author Christopher Tubbs
 *
 */
public final class Debug
{    
    public static void debug(Logger logger, String message)
    {
        debug(logger, message, System.err, new Object[0]);
    }
    
    public static void debug(Logger logger, String message, Object...objects)
    {
        debug(logger, message, System.err, objects);
    }
    
    public static void debug(Logger logger, String message, PrintStream outputStream, Object...objects)
    {
        if (canLog(logger, DEBUG))
        {
            logger.debug(message, objects);
        }
        else if (SystemSettings.isInDevelopment())
        {
            message = String.format(message, objects);
            
            if (message.length() > 1000)
            {
                message = message.substring(0, 1000) + "...";
            }
            
            outputStream.println(String.format(message, objects));
        }
    }
    
    public static void debug(Logger logger, String message, PrintStream outputStream)
    {
        debug(logger, message, outputStream, new Object[0]);
    }
    
    public static void debug(Logger logger, Exception error)
    {
        debug(logger, Strings.getStackTrace(error), System.err, new Object[0]);
    }
    
    public static void warn(Logger logger, String message, PrintStream outputStream, Object...objects)
    {
        if (canLog(logger, WARN))
        {
            logger.warn(message, objects);
        }
        else if (SystemSettings.isInDevelopment())
        {
            message = String.format(message, objects);
            
            if (message.length() > 1000)
            {
                message = message.substring(0, 1000) + "...";
            }
            
            outputStream.println(String.format(message, objects));
        }
    }
    
    public static void warn(Logger logger, String message)
    {
        warn(logger, message, System.err, new Object[0]);
    }
    
    public static void warn(Logger logger, String message, Object...objects)
    {
        warn(logger, message, System.err, objects);
    }
    
    public static void warn(Logger logger, String message, PrintStream outputStream)
    {
        warn(logger, message, outputStream, new Object[0]);
    }
    
    public static void error(Logger logger, String message, PrintStream outputStream, Object...objects)
    {
        try {
            if (canLog(logger, ERROR)) {
                logger.error(message, objects);
            } else if (SystemSettings.isInDevelopment()) {
                outputStream.println(String.format(message, objects));
            }
        }
        catch (Exception error)
        {
            System.err.println(String.format(message, objects));
            throw error;
        }
    }
    
    public static void error(Logger logger, Exception e)
    {
        error(logger, Strings.getStackTrace(e), System.err, new Object[0]);
    }
    
    public static void error(Logger logger, String message)
    {
        error(logger, message, System.err, new Object[0]);
    }
    
    public static void error(Logger logger, String message, Object...objects)
    {
        error(logger, message, System.err, objects);
    }
    
    public static void error(Logger logger, String message, PrintStream outputStream)
    {
        error(logger, message, outputStream, new Object[0]);
    }
    
    private static boolean canLog(Logger logger, byte logType)
    {
        boolean active = SystemSettings.shouldLogMessages() && logger != null;
        boolean canUseLogType = false;

        if (active)
        {
            switch (logType)
            {
                case WARN:
                    canUseLogType = logger.isWarnEnabled();
                    break;
                case DEBUG:
                    canUseLogType = logger.isDebugEnabled();
                    break;
                case ERROR:
                    canUseLogType = logger.isErrorEnabled();
                    break;
            }
        }
        
        return active && canUseLogType;
    }
    
    private static final byte WARN = 0;
    private static final byte DEBUG = 1;
    private static final byte ERROR = 2;
}
