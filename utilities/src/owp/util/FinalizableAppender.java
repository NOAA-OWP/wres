package owp.util;

/**
 * An interface with a {@link #finalize()} method. <br>
 * <br>
 * TODO: In old Log4j, when an appender was removed/detached, it would leave the file stream open. To force it to close,
 * I created this interface (under an older name) and then created a version of FileAppender that implemented this
 * interface. In the {@link #finalize()} method, it would close the stream and then that method would be called within
 * {@link LoggingTools#removeLoggingAppender(String, String)}. I do not know if Logback suffers from the same problem.
 * Check if it does; if the problem no longer exists, then remove this interface.
 * 
 * @author Hank.Herr
 */
public interface FinalizableAppender
{

    /**
     * Called when {@link LoggingTools#removeLoggingAppender(String, String)} is called in order to forcibly close any
     * open files.
     */
    public void finalize();
}
