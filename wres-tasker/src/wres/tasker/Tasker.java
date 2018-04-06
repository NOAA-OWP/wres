package wres.tasker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Tasker
{
    private static final Logger LOGGER = LoggerFactory.getLogger( Tasker.class );

    public static void main( String[] args )
    {
        LOGGER.info( "I will take wres job requests and queue them." );
    }
}
