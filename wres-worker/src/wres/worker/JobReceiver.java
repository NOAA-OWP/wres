package wres.worker;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.messages.generated.JobResult;

/**
 * The concrete class that does the work of taking a job message and creating
 * a WRES process to fulfil the job message's request.
 */

class JobReceiver extends DefaultConsumer
{
    private static final Logger LOGGER = LoggerFactory.getLogger( JobReceiver.class );

    private static final int META_FAILURE_CODE = 600;

    private final File wresExecutable;


    /**
     * Constructs a new instance and records its association to the passed-in channel.
     * @param channel the channel to which this consumer is attached
     * @param wresExecutable the wresExecutable to use for launching WRES
     */
    JobReceiver( Channel channel, File wresExecutable )
    {
        super( channel );
        this.wresExecutable = wresExecutable;
    }

    private File getWresExecutable()
    {
        return this.wresExecutable;
    }

    /**
     * This is the entry point that will accept a message and create a process.
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
        String message = new String( body, Charset.forName( "UTF-8" ) );
        LOGGER.info( "Received job message {}", message );

        // Translate the message into a command
        ProcessBuilder processBuilder = createBuilderFromMessage( message );

        // Check to see if there is any command at all.
        if ( processBuilder == null )
        {
            UnsupportedOperationException problem = new UnsupportedOperationException( "Could not execute due to invalid message." );
            LOGGER.warn( "", problem );
            byte[] response = JobReceiver.prepareMetaFailureResponse( new UnsupportedOperationException( problem ) );
            this.sendResponse( properties, response );
            return;
        }

        // Set up way to publish the standard output of the process to broker
        OutputMessenger stdoutMessenger =
                new OutputMessenger( this.getChannel().getConnection(),
                                     properties.getReplyTo(),
                                     properties.getCorrelationId(),
                                     OutputMessenger.WhichOutput.STDOUT );
        OutputMessenger stderrMessenger =
                new OutputMessenger( this.getChannel().getConnection(),
                                     properties.getReplyTo(),
                                     properties.getCorrelationId(),
                                     OutputMessenger.WhichOutput.STDERR );

        // Do the execution requested from the queue
        Process process;

        try
        {
            process = processBuilder.start();

            // Bind process outputs to message publishers
            stderrMessenger.accept( process.getErrorStream() );
            stdoutMessenger.accept( process.getInputStream() );
        }
        catch ( IOException ioe )
        {
            LOGGER.warn( "Failed to launch process from {}.", processBuilder, ioe );
            byte[] response = JobReceiver.prepareMetaFailureResponse( ioe );
            this.sendResponse( properties, response );
            return;
        }

        LOGGER.info( "Started subprocess {}", process );

        try
        {
            process.waitFor();
            int exitValue = process.exitValue();
            LOGGER.info( "Subprocess {} exited {}", process, exitValue );
            byte[] response;
            response = JobReceiver.prepareResponse( exitValue );
            this.sendResponse( properties, response );
        }
        catch ( InterruptedException ie )
        {
            LOGGER.warn( "Interrupted while waiting for {}.", process );
            byte[] response = JobReceiver.prepareMetaFailureResponse( ie );
            this.sendResponse( properties, response );
            Thread.currentThread().interrupt();
        }
    }


    /**
     * Translate a message from the queue into a ProcessBuilder to run
     * @param message a message from the queue
     * @return a ProcessBuilder to attempt to run, or null if invalid message
     */

    private ProcessBuilder createBuilderFromMessage( String message )
    {
        List<String> result = new ArrayList<>();

        String javaOpts = null;
        String executable = this.getWresExecutable().getPath();
        String command = "execute";
        String projectConfig = null;

        String[] messageParts = message.split( "," );

        for ( String messagePart : messageParts )
        {
            int indexOfEquals = messagePart.indexOf( '=' );

            if ( indexOfEquals < 1 || indexOfEquals == messagePart.length() )
            {
                LOGGER.warn( "Bad message, no equals here, or nothing after equals: '{}'", messagePart );
                return null;
            }

            String first = messagePart.substring( 0, indexOfEquals);
            String second = messagePart.substring( indexOfEquals + 1);

            if ( first.toUpperCase().equals( "JAVA_OPTS" ) )
            {
                javaOpts = second;
            }
            else if ( first.toLowerCase().equals( "projectconfig" ) )
            {
                projectConfig = second;
            }
        }

        // Make sure we have a project config...
        if ( projectConfig == null )
        {
            LOGGER.warn( "No project config specified in message." );
            return null;
        }

        result.add( executable );
        result.add( command );
        result.add( projectConfig );

        ProcessBuilder processBuilder = new ProcessBuilder( result );

        // Cause process builder to echo the subprocess's output when started.
        //processBuilder.inheritIO();
        // May not be able to set inheritIO when capturing stdout and stderr


        // Cause process builder to get java options if needed
        if ( javaOpts != null )
        {
            processBuilder.environment().put( "JAVA_OPTS", javaOpts );
        }

        return processBuilder;
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
     * @param jobProperties the properties of the job message, has queue and id
     * @param message the message to send.
     */
    private void sendResponse( AMQP.BasicProperties jobProperties, byte[] message )
    {
        AMQP.BasicProperties resultProperties =
                new AMQP.BasicProperties
                        .Builder()
                        .correlationId( jobProperties.getCorrelationId() )
                        .build();

        try
        {
            String exchangeName = jobProperties.getReplyTo();
            String exchangeType = "topic";
            String routingKey = "job." + jobProperties.getCorrelationId() + ".exitCode";

            this.getChannel().exchangeDeclare( exchangeName, exchangeType );

            this.getChannel().basicPublish( exchangeName,
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
    }
}
