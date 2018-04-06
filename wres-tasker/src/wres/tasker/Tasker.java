package wres.tasker;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Tasker
{
    private static final Logger LOGGER = LoggerFactory.getLogger( Tasker.class );
    private static final String SEND_QUEUE_NAME = "wres.job";


    /**
     * Tasker receives requests for wres runs and passes them along to queue.
     * @param args unused args
     * @throws IOException when communication with queue fails
     * @throws TimeoutException when connection to queue times out
     * @throws java.net.ConnectException when connection to queue fails
     */

    public static void main( String[] args )
            throws IOException, TimeoutException
    {
        LOGGER.info( "I will take wres job requests and queue them." );

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost( "localhost" );

        try ( Connection connection = factory.newConnection();
              Channel channel = connection.createChannel() )
        {
            channel.queueDeclare( SEND_QUEUE_NAME, false, false, false, null );
            String message = "Do some work! Launch WRES!";
            channel.basicPublish( "",
                                  SEND_QUEUE_NAME,
                                  null,
                                  message.getBytes() );
            LOGGER.info( "I sent a message to the queue." );
        }
    }
}
