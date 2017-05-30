package wres.util;

import java.io.IOException;

import ch.qos.logback.core.FileAppender;

// TODO Update for LogBack. Is this necessary???
/**
 * I'm not positive this is necessary, but it appears that Log4j does not close files to which it is told to write log
 * messages when the appender is removed. To compensate, I have this extend {@link FileAppender} but implement
 * {@link FinalizableAppender}, which includes a {@link FinalizableAppender#finalize()} method that is called whenever
 * the {@link LoggingTools#removeLoggingAppender(String, String)} is called.<Br>
 * <br>
 * This bug was found when running MEFPHindcaster, and can be seen by running lsof -p [pid] | grep "log". If you change
 * the appender used to be a standard {@link FileAppender} in the {@link LoggingTools} method, the list of files open
 * will grow.
 * 
 * @author hankherr
 */
public class OHDFileAppender extends FileAppender implements FinalizableAppender
{
    @Override
    public void finalize()
    {
        try
        {
            this.getOutputStream().flush();
            this.getOutputStream().close();
        }
        catch(final IOException e)
        {
            e.printStackTrace();
        }
    }
}
