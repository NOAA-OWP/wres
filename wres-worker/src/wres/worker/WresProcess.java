package wres.worker;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.messages.generated.JobResult;

class WresProcess implements Callable<Integer>
{
    private static final Logger LOGGER = LoggerFactory.getLogger( WresProcess.class );

    private static final int META_FAILURE_CODE = 600;

    private final ProcessBuilder processBuilder;
    private final String exchangeName;
    private final String jobId;
    private final Connection connection;

    WresProcess( ProcessBuilder processBuilder,
                 String exchangeName,
                 String jobId,
                 Connection connection )
    {
        this.processBuilder = processBuilder;
        this.exchangeName = exchangeName;
        this.jobId = jobId;
        this.connection = connection;
    }

    private ProcessBuilder getProcessBuilder()
    {
        return this.processBuilder;
    }

    private String getExchangeName()
    {
        return this.exchangeName;
    }

    private String getJobId()
    {
        return this.jobId;
    }

    private Connection getConnection()
    {
        return this.connection;
    }

    @Override
    public Integer call()
    {
        // Check to see if there is any command at all.
        if ( this.getProcessBuilder() == null )
        {
            UnsupportedOperationException problem = new UnsupportedOperationException( "Could not execute due to invalid message." );
            LOGGER.warn( "", problem );
            byte[] response = WresProcess.prepareMetaFailureResponse( new UnsupportedOperationException( problem ) );
            this.sendResponse( response );
            return META_FAILURE_CODE;
        }


        // Do the execution requested from the queue
        Process process;

        // Use one Thread per output stream:
        ExecutorService executorService = Executors.newFixedThreadPool( 2 );

        try
        {
            process = this.getProcessBuilder().start();

            // Set up way to publish the standard output of the process to broker
            // and bind process outputs to message publishers
            OutputMessenger stdoutMessenger =
                    new OutputMessenger( this.getConnection(),
                                         this.getExchangeName(),
                                         this.getJobId(),
                                         OutputMessenger.WhichOutput.STDOUT,
                                         process.getInputStream() );
            OutputMessenger stderrMessenger =
                    new OutputMessenger( this.getConnection(),
                                         this.getExchangeName(),
                                         this.getJobId(),
                                         OutputMessenger.WhichOutput.STDERR,
                                         process.getErrorStream() );
            executorService.submit( stdoutMessenger );
            executorService.submit( stderrMessenger );
        }
        catch ( IOException ioe )
        {
            LOGGER.warn( "Failed to launch process from {}.", processBuilder, ioe );
            byte[] response = WresProcess.prepareMetaFailureResponse( ioe );
            this.sendResponse( response );
            return META_FAILURE_CODE;
        }

        LOGGER.info( "Started subprocess {}", process );

        try
        {
            process.waitFor();
            int exitValue = process.exitValue();
            LOGGER.info( "Subprocess {} exited {}", process, exitValue );
            byte[] response;
            response = WresProcess.prepareResponse( exitValue );
            this.sendResponse( response );
            return exitValue;
        }
        catch ( InterruptedException ie )
        {
            LOGGER.warn( "Interrupted while waiting for {}.", process );
            byte[] response = WresProcess.prepareMetaFailureResponse( ie );
            this.sendResponse( response );
            Thread.currentThread().interrupt();
            return META_FAILURE_CODE;
        }
    }


    /**
     * Helper to prepare a completed job response
     * @param exitCode the actual exitCode of the wres process
     * @return raw message indicating job exit code
     */
    private static byte[] prepareResponse( int exitCode )
    {
        JobResult.job_result jobResult = JobResult.job_result
                .newBuilder()
                .setResult( exitCode )
                .build();
        return jobResult.toByteArray();
    }


    /**
     * Helper to prepare a job that failed due to never getting the job to run
     * @param e the exception that occurred or null if none
     * @return raw message indicating meta failure
     */

    private static byte[] prepareMetaFailureResponse( Exception e )
    {
        JobResult.job_result jobResult = JobResult.job_result
                .newBuilder()
                .setResult( META_FAILURE_CODE )
                .build();
        return jobResult.toByteArray();
    }

    /**
     * Attempts to send a message with job results to the queue specified in job
     * @param message the message to send.
     */
    private void sendResponse( byte[] message )
    {
        AMQP.BasicProperties resultProperties =
                new AMQP.BasicProperties
                        .Builder()
                        .correlationId( this.getJobId() )
                        .build();

        try ( Channel channel = this.getConnection().createChannel() )
        {
            String exchangeName = this.getExchangeName();
            String exchangeType = "topic";
            String routingKey = "job." + this.getJobId() + ".exitCode";

            channel.exchangeDeclare( exchangeName, exchangeType );

            channel.basicPublish( exchangeName,
                                  routingKey,
                                  resultProperties,
                                  message );
            LOGGER.info( "Seems like I published to exchange {}, key {}",
                         exchangeName, routingKey );
        }
        catch ( IOException ioe )
        {
            LOGGER.warn( "Sending this message failed: {}", message, ioe );
        }
        catch ( TimeoutException te )
        {
            LOGGER.warn( "Timed out sending this message: {}", message, te );
        }
    }


}
