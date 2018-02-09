package wres.io.thredds;

import org.eclipse.jetty.security.HashLoginService;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.webapp.Configuration;
import org.eclipse.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreddsFacade
{
    private static final Logger LOGGER = LoggerFactory.getLogger( ThreddsFacade.class );

    private ThreddsFacade()
    {
        // prevent construction until constructor args are known
    }

    public static void main( String[] args ) throws Exception
    {
        // tds.content.root.path has to be set, see
        // https://www.unidata.ucar.edu/software/thredds/v4.3/tds/tds4.2/faq.html
        String tmpPath = System.getProperty( "java.io.tmpdir" );
        LOGGER.info( "Using {} directory for thredds.", tmpPath );
        System.setProperty( "tds.content.root.path", tmpPath );

        // Following (more or less)
        // https://www.eclipse.org/jetty/documentation/current/embedded-examples.html#embedded-one-webapp

        /*
        // Fruitless attempt to get the THREDDS war from the classpath,
        // matching the version from build:
        URL warFromClasspath = ThreddsFacade.class
                .getClassLoader()
                .getResource( "tds-4.6.11.war" );
        LOGGER.info( "warFromClasspath: {}", warFromClasspath );
        Resource warFile = Resource.newResource( warFromClasspath.toString() );
        */

        // Gradle will have downloaded the war file for you, just put the
        // path here:
        Resource warFile = Resource.newResource( "/path/to/war/file/needed/here" );
        LOGGER.info( "warFile: {}, warFile.exists(): {}",
                     warFile, warFile.exists() );
        WebAppContext webAppContext = new WebAppContext( warFile, "/thredds" );

        webAppContext.setAttribute(
                "org.eclipse.jetty.server.webapp.ContainerIncludeJarPattern",
                ".*/[^/]*servlet-api-[^/]*\\.jar$|.*/javax.servlet.jsp.jstl-.*\\.jar$|.*/[^/]*taglibs.*\\.jar$" );

        Server server = new Server( 8080 );

        // For JSP?
        Configuration.ClassList classlist = Configuration.ClassList
                .setServerDefault( server );
        classlist.addBefore(
                "org.eclipse.jetty.webapp.JettyWebXmlConfiguration",
                "org.eclipse.jetty.annotations.AnnotationConfiguration" );

        // Default realm specified in thredds' web.xml login-conf:
        // "THREDDS Data Server" needs to match what we tell jetty
        HashLoginService loginService = new HashLoginService();
        loginService.setName( "THREDDS Data Server" );
        loginService.setConfig( "src/test/resources/realm.properties" );
        server.addBean( loginService );

        // Add the war to the server.
        server.setHandler( webAppContext );
        server.start();
        server.dumpStdErr();

        server.join();
    }
}
