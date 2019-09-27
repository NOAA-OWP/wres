package wres.worker;

import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.protobuf.Timestamp;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static wres.messages.generated.JobStatus.job_status.Report.*;

import wres.messages.generated.JobStatus;


/**
 * Produces and sends a message indicating the job was RECEIVED, and one ALIVE
 * message for every second (duration) that the process is alive, finally one
 * DEAD message when the process is not alive. Assumes the caller already
 * started the process and that it will be alive before it is dead. Nonetheless,
 * at a bare minimum (assuming connectivity to the broker works), a RECEIVED and
 * DEAD message will be sent even if the process was not started or was dead on
 * arrival to this class.
 *
 * A consumer (e.g. the tasker) can listen for this output on a topic named
 * with the convention job.[job_id].status, for example job.532.status,
 * and can do whatever it chooses with the messages such as display them, serve
 * them, or store them.
 */

public class JobStatusMessenger implements Runnable
{
    private static final Logger LOGGER = LoggerFactory.getLogger( JobStatusMessenger.class );
    private static final String TOPIC = "status";

    private final Connection connection;
    private final String exchangeName;
    private final String jobId;
    private final Process process;

    /** Helps the consumer re-order messages received out-of-order */
    private final AtomicInteger order = new AtomicInteger( 0 );

    JobStatusMessenger( Connection connection,
                        String exchangeName,
                        String jobId,
                        Process process )
    {
        this.connection = connection;
        this.exchangeName = exchangeName;
        this.jobId = jobId;
        this.process = process;
    }

    private Connection getConnection()
    {
        return this.connection;
    }

    private String getExchangeName()
    {
        return this.exchangeName;
    }

    private String getJobId()
    {
        return this.jobId;
    }

    private Process getProcess()
    {
        return this.process;
    }

    private AtomicInteger getOrder()
    {
        return this.order;
    }

    private String getRoutingKey()
    {
        return "job." + this.getJobId() + "." + TOPIC;
    }

    @Override
    public void run()
    {
        Process process = this.getProcess();

        try ( Channel channel = this.getConnection().createChannel() )
        {
            String exchangeName = this.getExchangeName();
            String exchangeType = "topic";

            channel.exchangeDeclare( exchangeName, exchangeType );

            this.sendMessage( channel, RECEIVED );

            while ( process.isAlive() )
            {
                this.sendMessage( channel, ALIVE );
                Thread.sleep( 1000 );
            }

            this.sendMessage( channel, DEAD );
        }
        catch ( TimeoutException te )
        {
            LOGGER.warn( "Failed to connect to broker.", te );
        }
        catch ( IOException ioe )
        {
            LOGGER.warn( "Failed to create channel or declare exchange.", ioe );
        }
        catch ( InterruptedException ie )
        {
            LOGGER.warn( "Interrupted while checking if process is alive.", ie );
            Thread.currentThread().interrupt();
        }
        LOGGER.info( "Finished sending {} for job {}", TOPIC, jobId );
    }


    /**
     * Attempts to send a message with a single line of output
     * @param channel the channel to use
     * @param report the status/report to send.
     */
    private void sendMessage( Channel channel, JobStatus.job_status.Report report )
    {
        AMQP.BasicProperties properties =
                new AMQP.BasicProperties
                        .Builder()
                        .correlationId( this.getJobId() )
                        .build();

        int order = this.getOrder().getAndIncrement();
        Instant now = Instant.now();
        Timestamp timestamp = Timestamp.newBuilder()
                                       .setSeconds( now.getEpochSecond() )
                                       .setNanos( now.getNano() )
                                       .build();
        JobStatus.job_status message
                = JobStatus.job_status
                .newBuilder()
                .setIndex( order )
                .setDatetime( timestamp )
                .setReport( report )
                .build();

        try
        {
            channel.basicPublish( this.getExchangeName(),
                                  this.getRoutingKey(),
                                  properties,
                                  message.toByteArray() );
        }
        catch ( IOException ioe )
        {
            LOGGER.warn( "Sending this output failed: {}", message, ioe );
        }
    }
}
