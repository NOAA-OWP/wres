package wres.worker;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.Map;
import java.util.HashMap;

import javax.net.ssl.SSLContext;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;

import com.rabbitmq.client.DefaultSaslConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.http.WebClient;
import wres.messages.BrokerHelper;

/**
 * A long-running, light-weight process that starts up and monitors a worker server, takes a job from a queue,
 * and sends the project config to its worker server, and repeats.
 */

public class Worker
{
    private static final Logger LOGGER = LoggerFactory.getLogger( Worker.class );
    private static final String RECV_QUEUE_NAME = "wres.job";

    /**
     * This code is used to signal something happened to the worker-server mid-evaluation.
     * We return this code instead of an exception so we can dequeue the job that likely caused this
     */
    private static final int META_FAILURE_CODE = 600;
    private static volatile boolean killed = false;

    private static final String SERVER_READY_FOR_WORK_CHECK_URI = "http://localhost:%d/evaluation/readyForWork";

    private static final Duration CALL_TIMEOUT = Duration.ofMinutes( 1 );

    private static final int DEFAULT_PORT = 8010;

    /** A web client to help with reading data from the web. */
    private static final WebClient WEB_CLIENT = new WebClient();

    /**
     * Expects exactly one arg with a path to WRES executable
     * @param args arguments, but only one is expected, a WRES executable
     * @throws IOException when communication with queue fails or process start fails.
     * @throws IllegalArgumentException when the first argument is not a WRES executable
     * @throws java.net.ConnectException when connection to queue fails.
     * @throws TimeoutException when connection to the queue times out.
     * @throws InterruptedException when interrupted while waiting for work.
     * @throws IllegalStateException when setting up our custom trust list fails
     */

    public static void main( String[] args )
            throws IOException, TimeoutException, InterruptedException
    {
        if ( args.length != 1 )
        {
            throw new IllegalArgumentException( "First arg must be an executable wres path." );
        }

        // Getting as a file allows us to verify it exists
        File wresExecutable = Paths.get( args[0] ).toFile();

        if ( !wresExecutable.exists() )
        {
            throw new IllegalArgumentException( "First arg must be an executable wres *path*." );
        }
        else if ( !wresExecutable.canExecute() )
        {
            throw new IllegalArgumentException( "First arg must be an *executable* wres path." );
        }

        // This is to satisfy SonarQube and IntelliJ IDEA re: infinite loop
        Runtime.getRuntime().addShutdownHook( new Thread( () -> {Worker.killed = true;} ) );

        // Set up connection parameters for connection to broker
        ConnectionFactory factory = createConnectionFactory();

        // Get work from the queue
        try ( Connection connection = factory.newConnection();
              Channel receiveChannel = connection.createChannel() )
        {
            // Take precisely one job at a time:
            receiveChannel.basicQos( 1 );

            Map<String, Object> queueArgs = new HashMap<String, Object>();
            queueArgs.put( "x-max-priority", 2 );
            receiveChannel.queueDeclare( RECV_QUEUE_NAME, true, false, false, queueArgs );

            BlockingQueue<WresEvaluationProcessor> processToLaunch = new ArrayBlockingQueue<>( 1 );

            Process serverProcess = startWorkerServer( wresExecutable );

            JobReceiver receiver = new JobReceiver( receiveChannel,
                                                    processToLaunch,
                                                    DEFAULT_PORT );

            receiveChannel.basicConsume( RECV_QUEUE_NAME, false, receiver );

            while ( !Worker.killed )
            {
                LOGGER.info( "Waiting for work..." );

                WresEvaluationProcessor wresEvaluationProcessor = processToLaunch.poll( 2, TimeUnit.MINUTES );

                if ( wresEvaluationProcessor != null && isServerReadyForWork() )
                {
                    // Launch WRES if the consumer found a message saying so.
                    Integer responseCode = wresEvaluationProcessor.call();
                    // Ack that this shim got and processed the message from the queue and remove that message
                    receiveChannel.basicAck( wresEvaluationProcessor.getDeliveryTag(), false );

                    // Something happened to the worker-server while evaluating, look for meta failure and throw exception
                    // We do this instead of passing the exception to be able to dequeue the job that caused this
                    if ( responseCode == META_FAILURE_CODE )
                    {
                        throw new EvaluationProcessingException(
                                "Something happened to the worker-server while processing the evaluation" );
                    }
                }
            }

            // When we break from this while loop it means the worker-shim is killed, kill its server too
            killServerProcess( serverProcess );
        }
        catch ( EvaluationProcessingException epe )
        {
            LOGGER.error( "There was an issue with the server while processing an evaluation" );
            throw epe;
        }
        catch ( IOException | TimeoutException | InterruptedException e )
        {
            String message = "Checked exception while talking to the broker";
            LOGGER.error( message, e );
            throw e;
        }
        catch ( RuntimeException re )
        {
            String message = "Unchecked exception while talking to the broker";
            LOGGER.error( message, re );
            throw re;
        }
        catch ( URISyntaxException e )
        {
            throw new RuntimeException( e );
        }
    }

    /**
     * Helper method to create a ConnectionFactory
     * @return ConnectionFactory
     */
    private static ConnectionFactory createConnectionFactory()
    {
        // Determine the actual broker name, whether from -D or default
        String brokerHost = BrokerHelper.getBrokerHost();
        String brokerVhost = BrokerHelper.getBrokerVhost();
        int brokerPort = BrokerHelper.getBrokerPort();

        // Set up connection parameters for connection to broker
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost( brokerHost );
        factory.setVirtualHost( brokerVhost );
        factory.setPort( brokerPort );
        factory.setSaslConfig( DefaultSaslConfig.EXTERNAL );
        factory.setAutomaticRecoveryEnabled( true );

        SSLContext sslContext =
                BrokerHelper.getSSLContextWithClientCertificate( BrokerHelper.Role.WORKER );
        factory.useSslProtocol( sslContext );

        return factory;
    }

    /**
     * Uses the wresExecutable passed in to start a worker server wrapped in a ProcessBuilder
     * @param wresExecutable
     * @return a Process containing a worker server
     */
    private static Process startWorkerServer( File wresExecutable )
    {
        String javaOpts = " ";
        List<String> serverProcessString = new ArrayList<>();

        // Pass through additional java options set in the environment for this
        // inner worker process, as distinct from this shim process.
        String innerJavaOpts = System.getenv( "INNER_JAVA_OPTS" );

        if ( innerJavaOpts != null && innerJavaOpts.length() > 0 )
        {
            javaOpts = innerJavaOpts;
        }

        String executable = wresExecutable
                .getPath();

        serverProcessString.add( executable );
        serverProcessString.add( "server" );
        serverProcessString.add( String.valueOf( DEFAULT_PORT ) );

        ProcessBuilder processBuilder = new ProcessBuilder( serverProcessString );
        processBuilder.environment().put( "JAVA_OPTS", javaOpts );

        Process process;

        try
        {
            // Start the server inheriting the IO so we can see it in the docker logs.
            // Projects will redirect this to themselves when they run
            process = processBuilder.inheritIO().start();

            if ( process.isAlive() )
            {
                return process;
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }

        // We cannot start a worker-server, kill the worker-shim
        Worker.killed = true;
        throw new InternalError( "Was unable to start up the worker server" );
    }


    /**
     * Method to check if the server is ready to accept a new job
     * @return boolean value if the server returns 200 (HTTP.OK)
     * @throws IOException, URISyntaxException
     */
    private static boolean isServerReadyForWork() throws IOException, URISyntaxException
    {
        URI uri = new URI( String.format( SERVER_READY_FOR_WORK_CHECK_URI, DEFAULT_PORT ) );
        try (
                WebClient.ClientResponse fromWeb = WEB_CLIENT.getFromWeb( uri, CALL_TIMEOUT );
        )
        {
            return fromWeb.getStatusCode() == HttpURLConnection.HTTP_OK;
        }
    }

    private static void killServerProcess( Process oldServerProcess )
    {
        if ( oldServerProcess.isAlive() )
        {
            oldServerProcess.destroy();
        }
    }
}
