package wres.tasker;

import org.glassfish.jersey.message.DeflateEncoder;
import org.glassfish.jersey.message.GZipEncoder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.filter.EncodingFilter;

import jakarta.ws.rs.ApplicationPath;

/**
 * This is where JAX-RS meets the servlet specification or something.
 * Practically, when we add more web service classes, we register them here,
 * and this is the class we tell the servlet container to use as our web app.
 */

@ApplicationPath( "/" )
public class JaxRSApplication extends ResourceConfig
{
    public JaxRSApplication()
    {
        this.register( WresJob.class );
        this.register( WresJobResult.class );
        this.register( WresJobStdout.class );
        this.register( WresJobStderr.class );
        this.register( WresJobInput.class );
        this.register( WresJobOutput.class );
        this.register( GZipEncoder.class );
        this.register( DeflateEncoder.class );

        EncodingFilter.enableFor( this, GZipEncoder.class );
    }
}
