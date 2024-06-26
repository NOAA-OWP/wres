package wres.system.logging;

import java.io.IOException;
import java.io.InputStream;
import java.util.logging.LogManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for java.util.logging that reads logging.properties from classpath.
 * Specified in the property -Djava.util.logging.config.class, as described in
 * https://docs.oracle.com/javase/8/docs/api/java/util/logging/LogManager.html
 * "that object's constructor is responsible for reading in the initial
 *  configuration"
 * This class should not need to exist (but java.util.logging is what it is).
 * TODO: JBr: this class probably isn't necessary, providing the relevant system property is set, 
 * -Djava.util.logging.config.file=logging.properties. More here, although the documentation is less clear for jdk11
 * than jdk8:
 * <a href="https://docs.oracle.com/javase/8/docs/api/java/util/logging/LogManager.html">jdk8</a>
 * <a href="https://docs.oracle.com/en/java/javase/11/docs/api/java.logging/java/util/logging/LogManager.html">jdk11</a>
 */
public class JavaUtilLoggingRedirector
{
    private static final Logger LOGGER = LoggerFactory.getLogger( JavaUtilLoggingRedirector.class );
    private static final String PROPERTIES_FILE_NAME = "logging.properties";

    /**
     * Creates an instance.
     */
    public JavaUtilLoggingRedirector()
    {
        LogManager theLogManager = LogManager.getLogManager();

        try ( InputStream julProperties =
                      JavaUtilLoggingRedirector.class
                                               .getClassLoader()
                                               .getResourceAsStream( PROPERTIES_FILE_NAME ) )
        {
            theLogManager.readConfiguration( julProperties );
        }
        catch ( IOException ioe )
        {
            LOGGER.warn( "Could not read java.util.logging-to-slf4j properties file {} on the classpath. Messages sent through j.u.l may be lost.",
                         PROPERTIES_FILE_NAME, ioe );
        }
    }
}
