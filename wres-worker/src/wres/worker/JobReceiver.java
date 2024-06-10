package wres.worker;

import java.util.concurrent.BlockingQueue;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The concrete class that does the work of taking a job message and creating
 * a WRES process to fulfil the job message's request.
 *
 * Uses environment variable JAVA_OPTS to set database details for a run,
 * appends environment variable INNER_JAVA_OPTS to JAVA_OPTS to set additional
 * -D parameters such as those related to logging.
 */

class JobReceiver extends DefaultConsumer
{
    private static final Logger LOGGER = LoggerFactory.getLogger( JobReceiver.class );

    private final BlockingQueue<WresEvaluationProcessor> processToLaunch;

    private final int port;

    /**
     * Constructs a new instance and records its association to the passed-in channel.
     * Some Junk mores
     * @param channel the channel to which this consumer is attached
     * @param processToLaunch a Q to send data back to the main thread with
     * @param port The port that the local server lives on, passing this through to the WresProcess
     */

    JobReceiver( Channel channel,
                 BlockingQueue<WresEvaluationProcessor> processToLaunch,
                 int port )
    {
        super( channel );
        this.processToLaunch = processToLaunch;
        this.port = port;
    }

    private BlockingQueue<WresEvaluationProcessor> getProcessToLaunch()
    {
        return this.processToLaunch;
    }

    private int getPort()
    {
        return this.port;
    }


    /**
     * This is the entry point that will accept a message and create a
     * WresProcess that the main thread or another thread can run, sharing it
     * with the creator of this JobReceiver via a blocking q.
     * @param consumerTag boilerplate
     * @param envelope boilerplate
     * @param properties boilerplate
     * @param body the message body of the job request from the queue
     */

    @Override
    public void handleDelivery( String consumerTag,
                                Envelope envelope,
                                AMQP.BasicProperties properties,
                                byte[] body )
    {

        // Set up the information needed to launch process and send info back
        WresEvaluationProcessor wresEvaluationProcessor = new WresEvaluationProcessor( properties.getReplyTo(),
                                                                                       properties.getCorrelationId(),
                                                                                       this.getChannel()
                                                                                           .getConnection(),
                                                                                       envelope,
                                                                                       body,
                                                                                       this.getPort() );
        // Share the process information with the caller
        boolean wasOffered = this.getProcessToLaunch().offer( wresEvaluationProcessor );

        if (!wasOffered) {
            throw new InternalError( "Unable to queue job" );
        }
    }
}
