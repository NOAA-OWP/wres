package wres.tasker;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;

/**
 * This is where JAX-RS meets the servlet specification or something.
 * Practically, when we add more web service classes, we register them here,
 * and this is the class we tell the servlet container to use as our web app.
 */

@ApplicationPath( "/" )
public class JaxRSApplication extends Application
{
    @Override
    public Set<Class<?>> getClasses()
    {
        Set<Class<?>> s = new HashSet<>( 2 );
        s.add( WresJob.class );
        s.add( WresJobResult.class );
        return Collections.unmodifiableSet( s );
    }
}
