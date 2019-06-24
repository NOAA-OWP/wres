package wres.tasker;

import java.util.concurrent.BlockingQueue;

import javax.xml.bind.DatatypeConverter;

import com.google.protobuf.InvalidProtocolBufferException;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.messages.generated.JobStandardStream;

class JobStandardStreamConsumer extends DefaultConsumer
{
    private static final Logger LOGGER = LoggerFactory.getLogger( JobStandardStreamConsumer.class );

    private static final int RETRY_COUNT = 9001;

    private final BlockingQueue<JobStandardStream.job_standard_stream> result;

    /**
     * @param channel the channel to which this consumer is attached
     * @param result the shared object to write a result to
     */

    JobStandardStreamConsumer( Channel channel,
                               BlockingQueue<JobStandardStream.job_standard_stream> result )
    {
        super( channel );
        this.result = result;
    }

    private BlockingQueue<JobStandardStream.job_standard_stream> getResult()
    {
        return this.result;
    }

    @Override
    public void handleDelivery( String consumerTag,
                                Envelope envelope,
                                AMQP.BasicProperties properties,
                                byte[] message )
    {
        LOGGER.debug( "Heard a message, consumerTag: {}, envelope: {}, properties: {}, message: {}",
                      consumerTag, envelope, properties, message );

        JobStandardStream.job_standard_stream decodedResult;

        try
        {
            decodedResult = JobStandardStream.job_standard_stream.parseFrom( message );
        }
        catch ( InvalidProtocolBufferException ipbe )
        {
            String hexVersion = DatatypeConverter.printHexBinary( message );
            throw new WresParseException( "Failed to parse message, hex: " + hexVersion, ipbe );
        }

        boolean offerSucceeded = this.getResult()
                                     .offer( decodedResult );
        int tries = 1;

        while ( !offerSucceeded && tries <= RETRY_COUNT )
        {
            LOGGER.info( "Failed to offer {} to the standardstream processing queue {}, retrying.",
                         decodedResult, this.getResult() );

            offerSucceeded = this.getResult()
                                 .offer( decodedResult );
            tries++;
        }

        if ( !offerSucceeded )
        {
            LOGGER.warn( "Failed to offer {} to the standardstream processing queue {} after {} tries, gave up.",
                         decodedResult, this.getResult(), tries );
        }
    }
}
