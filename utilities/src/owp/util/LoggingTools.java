package owp.util;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.PatternLayout;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.ConsoleAppender;
import ch.qos.logback.core.FileAppender;

/**
 * These logging tools all assume that the name of a {@link Logger} or {@link ch.qos.logback.classic.Logger} corresponds
 * to the package and class name of the class generating the message.
 * 
 * @author Hank.Herr
 */
public abstract class LoggingTools
{
    private final static Logger LOG = LoggerFactory.getLogger(LoggingTools.class);

    private final static String STANDARD_STDOUT_LOGGER_NAME = "stdoutConsoleAppender";
    private final static String STANDARD_LOGFILE_LOGGER_NAME = "logFileAppender";
    private final static String STANDARD_PATTERN_FORMAT = "%-5p [%c] - %m%n";

    /**
     * Creates a stdout Log4j logger that displays messages. The additivity of the logger is false, meaning that any
     * messages generated beneath the packageName will only be displayed by this appender its descendants.
     * 
     * @param packageName The package name to which to tie this appender.
     * @param messageLevel The level of messages to return, of type {@link Level}.
     * @return The name of the appender, which can be used to identify it later.
     */
    public static Logger initializeStdoutLogger(final String packageName, final Level messageLevel)
    {
        final LoggerContext context = (LoggerContext)LoggerFactory.getILoggerFactory();

        //Pattern
        final PatternLayout layout = new PatternLayout();
        layout.setContext(context);
        layout.setPattern(STANDARD_PATTERN_FORMAT);
        layout.start();

        //Appender
        final ConsoleAppender appender = new ConsoleAppender();
        appender.setContext(context);
        appender.setLayout(layout);
        appender.setTarget("System.out");
        appender.setName(STANDARD_STDOUT_LOGGER_NAME);
        appender.start();

        //Setup the Logger.
        final ch.qos.logback.classic.Logger logger =
                                                   (ch.qos.logback.classic.Logger)LoggerFactory.getLogger(packageName);
        if(logger.getAppender(STANDARD_STDOUT_LOGGER_NAME) == null)
        {
            logger.addAppender(appender);
            logger.setAdditive(false);
            logger.setLevel(messageLevel);
        }

        return logger;
    }

    /**
     * Initializes a logger to stdout at the {@link Level#DEBUG} level for the root package, so that every message is
     * caught (unless logger additivity of another logger prevents it).
     */
    public static Logger initializeStdoutLoggerForDebugging()
    {
        return initializeStdoutLogger(Logger.ROOT_LOGGER_NAME, Level.DEBUG);
    }

    /**
     * Initializes a logger to stdout at the {@link Level#DEBUG} level for the provided package, so that every message
     * is caught (unless logger additivity of another logger prevents it).
     * 
     * @param packageName The name of the package for logging.
     */
    public static Logger initializeStdoutLoggerForDebugging(final String packageName)
    {
        return initializeStdoutLogger(packageName, Level.DEBUG);
    }

    /**
     * Creates a log-file appending logger. The file appender is {@link OHDFileAppender}, which includes a forcible
     * closing method that will be called when {@link #removeLoggingAppender(String, String)} is called.
     * 
     * @param logFile The file in which to store messages.
     * @param packageName The name of the package to which to tie this appender.
     * @param messageLevel The level of messaging to put in the file.
     * @param additive Message additivity to use. When true, some extra messaging appears to be generated to stdout and
     *            I'm not sure why.
     * @param deleteFile If true, then an existing file by that name is deleted when this logger is initialized.
     * @return The logger created.
     * @throws IOException If a problem occurs opening the file.
     */
    public static Logger initializeLogFileLogger(final File logFile,
                                                 final String packageName,
                                                 final Level messageLevel,
                                                 final boolean additive,
                                                 final boolean deleteFile) throws IOException
    {
        //Delete the file if requested.
        if(deleteFile)
        {
            if(logFile.exists())
            {
                if(!logFile.delete())
                {
                    LOG.warn("Unable to remove the log file " + logFile.getAbsolutePath()
                        + " upon initializing new log file logger; will append messages to file.");
                }
            }
        }
        final LoggerContext context = (LoggerContext)LoggerFactory.getILoggerFactory();

        final PatternLayout layout = new PatternLayout();
        layout.setContext(context);
        layout.setPattern(STANDARD_PATTERN_FORMAT);
        layout.start();

        final FileAppender appender = new OHDFileAppender();
        appender.setFile(logFile.getAbsolutePath());
        appender.setContext(context);
        appender.setLayout(layout);
        appender.setName(STANDARD_LOGFILE_LOGGER_NAME);
        appender.start();

        final ch.qos.logback.classic.Logger logger =
                                                   (ch.qos.logback.classic.Logger)LoggerFactory.getLogger(packageName);
        if(logger.getAppender(STANDARD_LOGFILE_LOGGER_NAME) == null)
        {
            logger.addAppender(appender);
            logger.setAdditive(false);
            logger.setLevel(messageLevel);
        }
        return logger;

    }

    /**
     * Creates a log-file appending logger. Assumes the additivity is false, so that messages are cutoff, and that the
     * log file is being created from scratch, so that deleteFile is true..
     * 
     * @param logFile The file in which to store messages.
     * @param packageName The name of the package to which to tie this appender.
     * @param level The level of messaging to put in the file.
     * @return The name of the appender, which can be used to identify it later.
     * @throws IOException If a problem occurs opening the file.
     */
    public static Logger initializeLogFileLogger(final File logFile,
                                                 final String packageName,
                                                 final Level level) throws IOException
    {
        return initializeLogFileLogger(logFile, packageName, level, false, true);
    }

    /**
     * @param appenderName Name of the {@link Appender} to remove.
     * @param packageName The package with which the appender is associated.
     */
    public static void removeLoggingAppender(final String appenderName, final String packageName)
    {
        final ch.qos.logback.classic.Logger logger =
                                                   ((ch.qos.logback.classic.Logger)LoggerFactory.getLogger(packageName));
        final Appender app = logger.getAppender(appenderName);
        ((ch.qos.logback.classic.Logger)LoggerFactory.getLogger(packageName)).detachAppender(app);
        if(app instanceof FinalizableAppender)
        {
            ((FinalizableAppender)app).finalize();
        }
    }

    /**
     * Calls {@link #removeLoggingAppender(String, String)} for logger with name {@link #STANDARD_STDOUT_LOGGER_NAME}.
     * 
     * @param packageName The package with which the appender is associated.
     */
    public static void removeStdoutLogger(final String packageName)
    {
        removeLoggingAppender(STANDARD_STDOUT_LOGGER_NAME, packageName);
    }

    /**
     * Calls {@link #removeLoggingAppender(String, String)} for logger with name {@link #STANDARD_LOGFILE_LOGGER_NAME}.
     * 
     * @param packageName The package with which the appender is associated.
     */
    public static void removeLogFileLogger(final String packageName)
    {
        removeLoggingAppender(STANDARD_LOGFILE_LOGGER_NAME, packageName);
    }

    /**
     * Dumps the stack trace for the given {@link Throwable} as debug level messages in the provided {@link Logger}.
     * Only lineCount many lines are output.
     * 
     * @param log The {@link Logger} to which messages are posted.
     * @param t The {@link Throwable} from which the stack trace is drawn.
     * @param lineCount The number of lines to output to the log.
     */
    public static void outputStackTraceAsDebug(final Logger log, final Throwable t, final int lineCount)
    {
        final List<StackTraceElement> outputItems = Arrays.asList(t.getStackTrace()).subList(0, lineCount);
        LoggingTools.outputDebugLines(log, outputItems);
        if(outputItems.size() < t.getStackTrace().length)
        {
            outputDebugLines(log, "... [additional lines not shown]");
        }
    }

    /**
     * Dumps the stack trace for the given {@link Throwable} as debug level messages in the provided {@link Logger}.
     */
    public static void outputStackTraceAsDebug(final Logger log, final Throwable t)
    {
        LoggingTools.outputDebugLines(log, ExceptionTools.multiLineStackTrace(t));
    }

    /**
     * As {@link #outputStackTraceAsDebug(Logger, Throwable, int)}, but the message level is info.
     */
    public static void outputStackTraceAsInfo(final Logger log, final Throwable t, final int lineCount)
    {
        final List<StackTraceElement> outputItems = Arrays.asList(t.getStackTrace()).subList(0, lineCount);
        LoggingTools.outputInfoLines(log, outputItems);
        if(outputItems.size() < t.getStackTrace().length)
        {
            outputInfoLines(log, "... [additional lines not shown]");
        }
    }

    /**
     * As {@link #outputStackTraceAsDebug(Logger, Throwable)}, but the message level is info.
     */
    public static void outputStackTraceAsInfo(final Logger log, final Throwable t)
    {
        LoggingTools.outputInfoLines(log, ExceptionTools.multiLineStackTrace(t));
    }

    /**
     * Calls {@link #outputDebugLines(Logger, Object[])}.
     */
    public static void outputDebugLines(final Logger log, final String fullString)
    {
        final String lines[] = fullString.split("\\r?\\n");
        outputDebugLines(log, lines);
    }

    /**
     * Calls {@link #outputInfoLines(Logger, Object[])}.
     */
    public static void outputInfoLines(final Logger log, final String fullString)
    {
        final String lines[] = fullString.split("\\r?\\n");
        outputInfoLines(log, lines);
    }

    /**
     * Calls {@link #outputWarnLines(Logger, Object[])}.
     */
    public static void outputWarnLines(final Logger log, final String fullString)
    {
        final String lines[] = fullString.split("\\r?\\n");
        outputWarnLines(log, lines);
    }

    /**
     * Calls {@link #outputErrorLines(Logger, Object[])}.
     */
    public static void outputErrorLines(final Logger log, final String fullString)
    {
        final String lines[] = fullString.split("\\r?\\n");
        outputErrorLines(log, lines);
    }

    /**
     * Output multiple lines calling {@link Logger#debug(String)}.
     * 
     * @param log The {@link Logger} to use.
     * @param lines The lines to output.
     */
    public static void outputDebugLines(final Logger log, final Object[] lines)
    {
        for(final Object line: lines)
        {
            log.debug(line.toString().replaceAll("\\s+$", ""));
        }
    }

    /**
     * Output multiple lines calling {@link Logger#info(String)}.
     * 
     * @param log The {@link Logger} to use.
     * @param lines The lines to output.
     */
    public static void outputInfoLines(final Logger log, final Object[] lines)
    {
        for(final Object line: lines)
        {
            log.info(line.toString());
        }
    }

    /**
     * Output multiple lines calling {@link Logger#warn(String)}.
     * 
     * @param log The {@link Logger} to use.
     * @param lines The lines to output.
     */
    public static void outputWarnLines(final Logger log, final Object[] lines)
    {
        for(final Object line: lines)
        {
            log.warn(line.toString());
        }
    }

    /**
     * Output multiple lines calling {@link Logger#error(String)}.
     * 
     * @param log The {@link Logger} to use.
     * @param lines The lines to output.
     */
    public static void outputErrorLines(final Logger log, final Object[] lines)
    {
        for(final Object line: lines)
        {
            log.error(line.toString());
        }
    }
    /**
     * Output multiple lines calling {@link Logger#debug(String)}.
     * 
     * @param log The {@link Logger} to use.
     * @param lines The lines to output.
     */
    public static void outputDebugLines(final Logger log, final List<StackTraceElement> lines)
    {
        for(final StackTraceElement line: lines)
        {
            log.debug(line.toString().replaceAll("\\s+$", ""));
        }
    }

    /**
     * Output multiple lines calling {@link Logger#info(String)}.
     * 
     * @param log The {@link Logger} to use.
     * @param lines The lines to output.
     */
    public static void outputInfoLines(final Logger log, final List<StackTraceElement> lines)
    {
        for(final StackTraceElement line: lines)
        {
            log.info(line.toString());
        }
    }

    /**
     * Output multiple lines calling {@link Logger#warn(String)}.
     * 
     * @param log The {@link Logger} to use.
     * @param lines The lines to output.
     */
    public static void outputWarnLines(final Logger log, final List<StackTraceElement> lines)
    {
        for(final StackTraceElement line: lines)
        {
            log.warn(line.toString());
        }
    }

    /**
     * Output multiple lines calling {@link Logger#error(String)}.
     * 
     * @param log The {@link Logger} to use.
     * @param lines The lines to output.
     */
    public static void outputErrorLines(final Logger log, final List<StackTraceElement> lines)
    {
        for(final StackTraceElement line: lines)
        {
            log.error(line.toString());
        }
    }
}
