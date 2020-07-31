package wres.events;

import java.util.Objects;

import javax.jms.ExceptionListener;
import javax.jms.JMSException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Listen for failures on a connection.
 */

class ConnectionExceptionListener implements ExceptionListener
{
    private static final Logger LOGGER = LoggerFactory.getLogger( ConnectionExceptionListener.class );

    /**
     * The client that encountered the exception.
     */

    private final String clientId;

    @Override
    public void onException( JMSException exception )
    {
        LOGGER.error( "An exception listener uncovered an error in client {}. {}",
                      this.clientId,
                      exception.getMessage() );
    }

    /**
     * Creates an instance with an evaluation identifier and a message client identifier.
     * 
     * @param evaluationId the evaluation identifier
     * @param clientId the client identifier
     */

    ConnectionExceptionListener( String clientId )
    {
        Objects.requireNonNull( clientId );

        this.clientId = clientId;
    }

}
