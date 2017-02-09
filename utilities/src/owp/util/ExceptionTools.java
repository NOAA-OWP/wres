package owp.util;

import java.io.PrintWriter;
import java.io.StringWriter;

public abstract class ExceptionTools
{

    /**
     * A method that captures stack trace into a string and prints it in a multi-line fashion
     * 
     * @param e The {@link Throwable} that contains the stack trace that is to be parsed into a String.
     * @return {@link String} containing stack trace dumped to lines.
     */
    public static String multiLineStackTrace(final Throwable e)
    {
        final StringWriter sw = new StringWriter();
        final PrintWriter pr = new PrintWriter(sw);
        e.printStackTrace(pr);
        final String stacktrace = sw.toString();
        return stacktrace;
    }
}