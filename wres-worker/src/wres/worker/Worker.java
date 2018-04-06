package wres.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Worker
{
    private static final Logger LOGGER = LoggerFactory.getLogger( Worker.class );

    public static void main( String[] args )
    {
        LOGGER.info( "Hello, what is your bidding?" );
    }
}
