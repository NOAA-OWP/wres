package owp.util;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import junit.framework.TestCase;
import owp.util.LoggingTools;

public class LoggingToolsTest extends TestCase
{
    /**
     * Basic example sends a Hello world to stdout.
     */
    public void test1LogbackStdoutExample()
    {
        LoggingTools.initializeStdoutLoggerForDebugging("utilities.logging");

        final Logger logger = LoggerFactory.getLogger(LoggingToolsTest.class);
        logger.info("Hello world.");

        LoggingTools.removeStdoutLogger("utilities.logging");
    }

    /**
     * Creates a log file with level info and sends messages of varying levels to it.
     */
    public void test2LogFileCreationAndRemoval()
    {
        try
        {
            LoggingTools.initializeLogFileLogger(new File("testoutput/loggingToolsTest/test2.log"),
                                                 "utilities.logging",
                                                 Level.INFO);

            final Logger logger = LoggerFactory.getLogger(LoggingToolsTest.class);
            logger.info("First message.");
            logger.debug("Second message.");
            logger.warn("Third message.");
            logger.error("Fourth message.");
            logger.info("There should be no debug message.");

            LoggingTools.removeLogFileLogger("utilities.lgging");
        }
        catch(final IOException e)
        {
            e.printStackTrace();
            fail("Unexpected exception.");
        }

        //Need to add a file-benchmark comparison tool.  
        //I have one in ohdcommonchps under ohd.hseb.hefs.utils.junit, but am not sure if we should use it.

    }
}
