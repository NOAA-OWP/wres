package wres.vis;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple forwarding class to catch Log4j messages and forward them using standard the standard {@link Logger} so that
 * other tools can pickup on the messages.  This can handle debug, info, warning, and error messages only.
 * 
 * @author Hank.Herr
 */
public class Log4jMessageForwarderAppender extends AppenderSkeleton
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Log4jMessageForwarderAppender.class);
    
    public Log4jMessageForwarderAppender()
    {
        this.setName(Log4jMessageForwarderAppender.class.getName());
    }
    
    @Override
    protected void append(final LoggingEvent evt)
    {
        if(evt.getLevel() == Level.DEBUG)
        {
            LOGGER.debug("[from " + evt.getLoggerName() + "] " + evt.getRenderedMessage());
        }
        else if(evt.getLevel() == Level.ERROR)
        {
            LOGGER.error("[from " + evt.getLoggerName() + "] " + evt.getRenderedMessage());
        }
        else if(evt.getLevel() == Level.INFO)
        {
            LOGGER.info("[from " + evt.getLoggerName() + "] " + evt.getRenderedMessage());
        }
        else if(evt.getLevel() == Level.WARN)
        {
            LOGGER.warn("[from " + evt.getLoggerName() + "] " + evt.getRenderedMessage());
        }
    }

    @Override
    public void close()
    {
    }

    @Override
    public boolean requiresLayout()
    {
        return false;
    }

    /**
     * Sets up a {@link Log4jMessageForwarderAppender} appender to catch Log4j messages and forward so that LogBack or
     * other tools can see them. It prefixes test to the message indicating that it is a forwarded messages.
     * 
     * @param packageName The name of the package for which all messages from that package and below are caught and
     *            forwarded.
     */
    public static void setupMessageForwarding(final String packageName)
    {
        final org.apache.log4j.Logger logForwarder = org.apache.log4j.Logger.getLogger(packageName);
        logForwarder.addAppender(new Log4jMessageForwarderAppender());
        logForwarder.setAdditivity(false); //Cutoff logging so that nothing gets to CHPS logs panel if false.
        logForwarder.setLevel(Level.DEBUG);
    }
    
    /**
     * Removes the {@link Log4jMessageForwarderAppender} associated with the provided package name.
     */
    public static void removeMessageForwarding(final String packageName)
    {
        final org.apache.log4j.Logger logForwarder = org.apache.log4j.Logger.getLogger(packageName);
        logForwarder.removeAppender(Log4jMessageForwarderAppender.class.getName());
    }
}
