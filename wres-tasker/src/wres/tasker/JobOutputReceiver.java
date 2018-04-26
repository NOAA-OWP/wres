package wres.tasker;

import java.nio.charset.Charset;
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

class JobOutputReceiver extends DefaultConsumer
{
    private static final Logger LOGGER = LoggerFactory.getLogger( JobOutputReceiver.class );

    private final BlockingQueue<JobStandardStream.job_standard_stream> result;

    /**
     * @param channel the channel to which this consumer is attached
     * @param result the shared object to write a result to
     */

    JobOutputReceiver( Channel channel,
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

        this.getResult().offer( decodedResult );
    }

    /**
     * Because of the lack of checked exception handling above, use custom
     * RuntimeException to notify when a parse error occurs (which means that
     * we probably messed up somewhere in versions of a dependency or something)
     */
    private static class WresParseException extends RuntimeException
    {
        public WresParseException( Throwable cause )
        {
            super( cause );
        }

        public WresParseException( String message, Throwable cause )
        {
            super( message, cause );
        }

        public WresParseException( String message )
        {
            super( message );
        }
    }
}
