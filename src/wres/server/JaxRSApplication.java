package wres.server;

import jakarta.ws.rs.ApplicationPath;
import org.glassfish.jersey.message.DeflateEncoder;
import org.glassfish.jersey.message.GZipEncoder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.filter.EncodingFilter;

/**
 * This is where JAX-RS meets the servlet specification or something.
 * Practically, when we add more web service classes, we register them here,
 * and this is the class we tell the servlet container to use as our web app.
 */

@ApplicationPath( "/" )
public class JaxRSApplication extends ResourceConfig
{
    /**
     * Create an application instance.
     */
    public JaxRSApplication()
    {
        this.register( ProjectService.class );
        this.register( GZipEncoder.class );
        this.register( DeflateEncoder.class );

        EncodingFilter.enableFor( this, GZipEncoder.class );
    }
}
