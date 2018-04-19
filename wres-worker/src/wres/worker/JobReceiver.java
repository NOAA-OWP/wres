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


/**
 * The concrete class that does the work of taking a job message and creating
 * a WRES process to fulfil the job message's request.
 */

class JobReceiver extends DefaultConsumer
{
    private static final Logger LOGGER = LoggerFactory.getLogger( JobReceiver.class );

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

        String response = "Message failed: '" + message + "'";

        // Translate the message into a command
        ProcessBuilder processBuilder = createBuilderFromMessage( message );

        // Check to see if there is any command at all.
        if ( processBuilder == null )
        {
            String problem = "Could not execute due to invalid message.";
            LOGGER.warn( problem );
            this.sendResponse( properties, response + " " + problem );
            return;
        }

        // Do the execution requested from the queue
        Process process;

        try
        {
            process = processBuilder.start();
        }
        catch ( IOException ioe )
        {
            LOGGER.warn( "Failed to launch process from {}.", processBuilder, ioe );
            String problem = "Failed to launch due to " + ioe.getMessage();
            this.sendResponse( properties, response + " " + problem );
            return;
        }

        LOGGER.info( "Started subprocess {}", process );

        try
        {
            process.waitFor();
            int exitValue = process.exitValue();
            LOGGER.info( "Subprocess {} exited {}", process, exitValue );
            this.sendResponse( properties, "Result of execution: " + exitValue );
        }
        catch ( InterruptedException ie )
        {
            LOGGER.warn( "Interrupted while waiting for {}.", process );
            this.sendResponse( properties, "Interrupted, JobReceiver dying!" );
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
        processBuilder.inheritIO();

        // Cause process builder to get java options if needed
        if ( javaOpts != null )
        {
            processBuilder.environment().put( "JAVA_OPTS", javaOpts );
        }

        return processBuilder;
    }


    /**
     * Attempts to send a message with job results to the queue specified in job
     * @param jobProperties the properties of the job message, has queue and id
     * @param message the message to send.
     */
    private void sendResponse( AMQP.BasicProperties jobProperties, String message )
    {
        AMQP.BasicProperties resultProperties =
                new AMQP.BasicProperties
                        .Builder()
                        .correlationId( jobProperties.getCorrelationId() )
                        .build();

        try
        {
            this.getChannel().basicPublish( "",
                                            //jobProperties.getReplyTo(),
                                            "",
                                            resultProperties,
                                            message.getBytes() );
        }
        catch ( IOException ioe )
        {
            LOGGER.warn( "Sending this message failed: {}", message, ioe );
        }
    }
}
