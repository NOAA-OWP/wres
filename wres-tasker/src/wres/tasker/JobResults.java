package wres.tasker;

import java.io.IOException;
import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Used to get information about results and where to find them.
 * Eagerly looks in job result queue when a job id is registered, so that a
 * call from a web service will have the complete result.
 */

class JobResults
{
    private static final Logger LOGGER = LoggerFactory.getLogger( JobResults.class );

    /** A shared bag of job results by ID */
    private static final ConcurrentMap<String,Integer> JOB_RESULTS_BY_ID = new ConcurrentHashMap<>();

    /** A magic value placeholder for job registered-but-not-done */
    private static final Integer JOB_NOT_DONE_YET = Integer.MIN_VALUE + 1;

    /** A shared bag of job standard out */
    private static final ConcurrentMap<String, List<String>> JOB_STDOUT_BY_ID = new ConcurrentHashMap<>();

    /** A shared bag of job standard error */
    private static final ConcurrentMap<String, List<String>> JOB_STDERR_BY_ID = new ConcurrentHashMap<>();

    /**
     * How many job results to look for at once (should probably be at least as
     * many as the count of workers active on the platform and at most the max
     * count of jobs expected to be in the job queue at any given time?)
     */
    private final int NUMBER_OF_THREADS = 25;

    /** An executor service that consumes job results and stores them in JOB_RESULTS_BY_ID. */
    private final ExecutorService EXECUTOR = Executors.newFixedThreadPool( NUMBER_OF_THREADS );

    /** The factory to get connections from, configured to reach broker */
    private final ConnectionFactory connectionFactory;

    /** The connection to use for retrieving results, long lived */
    private Connection connection;

    /** The lock to guard connection when init fails on construction */
    private final Object CONNECTION_LOCK = new Object();


    JobResults( ConnectionFactory connectionFactory )
    {
        this.connectionFactory = connectionFactory;
        // Will lazily initialize connection since trying here first requires
        // retry later anyway.
        this.connection = null;
    }

    private ConnectionFactory getConnectionFactory()
    {
        return this.connectionFactory;
    }

    Connection getConnection()
            throws IOException, TimeoutException
    {
        synchronized ( CONNECTION_LOCK )
        {
            if ( this.connection == null )
            {
                this.connection = this.getConnectionFactory()
                                      .newConnection();
            }
        }
        return this.connection;
    }

    /**
     * Get the results exchange name to get results from workers from.
     * @return a results queue name to use
     */

    static String getJobStatusExchangeName()
    {
        return "wres.job.status";
    }


    /**
     * Watches for the result code of a job regardless of if anyone actually
     * is looking for it. Idea is to pro-actively seek the results of jobs
     * on the service end and cache them before the web service is called.
     */

    private static class ResultWatcher implements Callable<Integer>
    {
        private final Connection connection;
        private final String jobStatusExchangeName;
        private final String jobId;

        /**
         * @param connection shared connection
         * @param jobStatusExchangeName the exchange name to look in
         * @param jobId the job identifier to look for
         */
        ResultWatcher( Connection connection,
                       String jobStatusExchangeName,
                       String jobId )
        {
            this.connection = connection;
            this.jobStatusExchangeName = jobStatusExchangeName;
            this.jobId = jobId;
            LOGGER.debug( "Instantiated {}", this );
        }

        private Connection getConnection()
        {
            return this.connection;
        }

        private String getJobStatusExchangeName()
        {
            return this.jobStatusExchangeName;
        }

        private String getJobId()
        {
            return this.jobId;
        }


        /**
         * @return the result of the job id (correlation id) or Integer.MIN_VALUE when interrupted
         * @throws IOException when queue declaration fails
         */

        public Integer call() throws IOException, TimeoutException
        {
            LOGGER.debug( "call called on {}", this );

            BlockingQueue<Integer> result = new ArrayBlockingQueue<>( 1 );

            int resultValue = Integer.MIN_VALUE;
            String exchangeName = this.getJobStatusExchangeName();
            String exchangeType = "topic";
            String bindingKey = "job." + this.getJobId() + ".exitCode";

            try ( Channel channel = this.getConnection().createChannel() )
            {
                channel.exchangeDeclare( exchangeName, exchangeType );

                // As the consumer, I want an exclusive queue for me?
                String queueName = channel.queueDeclare().getQueue();
                // Does this have any effect?
                AMQP.Queue.BindOk bindResult = channel.queueBind( queueName, exchangeName, bindingKey );

                LOGGER.debug( "Bindresult: {}", bindResult );

                JobResultReceiver jobResultReceiver =
                        new JobResultReceiver( channel,
                                               result );

                String consumerTag = channel.basicConsume( queueName,
                                                           true,
                                                           jobResultReceiver );
                LOGGER.debug( "consumerTag: {}", consumerTag );

                LOGGER.debug( "Waiting to take a result value..." );
                resultValue = result.take();
                LOGGER.debug( "Finished taking a result value." );
            }
            catch ( InterruptedException ie )
            {
                LOGGER.warn( "Interrupted while getting result for job {}, returning potentially fake result {}",
                             jobId, resultValue );
                Thread.currentThread().interrupt();
            }
            catch ( IOException ioe )
            {
                // Since we may or may not actually consume result, log exception here
                LOGGER.warn( "When attempting to get job results message using {}:",
                             this, ioe );
                throw ioe;
            }

            // Store state back out to the static bag-o-state. A better way?
            JOB_RESULTS_BY_ID.put( jobId, resultValue );

            return resultValue;
        }

        @Override
        public String toString()
        {
            StringJoiner result = new StringJoiner( ", ", "ResultWatcher with ", "" );
            result.add( "connection=" + this.getConnection() );
            result.add( "jobStatusExchangeName=" + this.getJobStatusExchangeName() );
            result.add( "jobId=" + this.getJobId() );
            return result.toString();
        }
    }


    /**
     * Watches for the stdout|stderr of a job regardless of if anyone actually
     * is looking for it. Idea is to pro-actively seek the results of jobs
     * on the service end and cache them before the web service is called.
     */

    private static class OutputWatcher implements Callable<List<String>>
    {
        private static final int LOCAL_Q_SIZE = 10;
        private static final int JOB_WAIT_MINUTES = 5;
        private static final int MESSAGE_WAIT_SECONDS = 5;

        public enum WhichOutput
        {
            STDOUT,
            STDERR;
        }

        private final Connection connection;
        private final String jobStatusExchangeName;
        private final String jobId;
        private final WhichOutput whichOutput;

        /**
         * @param connection shared connection
         * @param jobStatusExchangeName the exchange name to look in
         * @param jobId the job identifier to look for
         */
        OutputWatcher( Connection connection,
                       String jobStatusExchangeName,
                       String jobId,
                       WhichOutput whichOutput )
        {
            this.connection = connection;
            this.jobStatusExchangeName = jobStatusExchangeName;
            this.jobId = jobId;
            this.whichOutput = whichOutput;
            LOGGER.debug( "Instantiated {}", this );
        }

        private Connection getConnection()
        {
            return this.connection;
        }

        private String getJobStatusExchangeName()
        {
            return this.jobStatusExchangeName;
        }

        private String getJobId()
        {
            return this.jobId;
        }

        private WhichOutput getWhichOutput()
        {
            return this.whichOutput;
        }

        /**
         * @return the stdout|stderr of the job id (correlation id) or potentially empty list when interrupted
         * @throws IOException when queue declaration fails
         */

        public List<String> call() throws IOException, TimeoutException
        {
            List<String> sharedList = new CopyOnWriteArrayList<>();

            // Store shared list to the static bag-o-state.
            if ( this.getWhichOutput().equals( WhichOutput.STDOUT ) )
            {
                JOB_STDOUT_BY_ID.put( this.getJobId(), sharedList );
            }
            else if ( this.getWhichOutput().equals( WhichOutput.STDERR ) )
            {
                JOB_STDERR_BY_ID.put( this.getJobId(), sharedList );
            }
            else
            {
                throw new UnsupportedOperationException( "Output has to be stderr or stdout." );
            }

            BlockingQueue<String> oneLineOfOutput = new ArrayBlockingQueue<>( LOCAL_Q_SIZE );

            String exchangeName = this.getJobStatusExchangeName();
            String exchangeType = "topic";
            String bindingKey = "job." + this.getJobId() + "." + this.getWhichOutput().name();

            try ( Channel channel = this.getConnection().createChannel() )
            {
                channel.exchangeDeclare( exchangeName, exchangeType );

                // As the consumer, I want an exclusive queue for me.
                String queueName = channel.queueDeclare().getQueue();
                channel.queueBind( queueName, exchangeName, bindingKey );

                JobOutputReceiver jobOutputReceiver =
                        new JobOutputReceiver( channel,
                                               oneLineOfOutput );

                boolean timedOut = false;

                while ( !timedOut )
                {
                    channel.basicConsume( queueName,
                                          true,
                                          jobOutputReceiver );

                    LOGGER.debug( "Consumed from {}, waiting for result.", queueName );

                    // One call to .basicConsume can result in many messages
                    // being received by our jobOutputReceiver. Look for them.
                    // This still seems uncertain and finnicky, but works?
                    boolean mayBeMoreMessages = true;

                    while ( mayBeMoreMessages )
                    {
                        String oneMoreLine = oneLineOfOutput.poll( MESSAGE_WAIT_SECONDS, TimeUnit.SECONDS );

                        if ( oneMoreLine != null )
                        {
                            sharedList.add( oneMoreLine );
                        }
                        else
                        {
                            mayBeMoreMessages = false;
                        }
                    }

                    // Give up waiting if we don't get any output for some time.
                    String oneLastLine = oneLineOfOutput.poll( JOB_WAIT_MINUTES, TimeUnit.MINUTES );

                    if ( oneLastLine != null )
                    {
                        sharedList.add( oneLastLine );
                    }
                    else
                    {
                        timedOut = true;
                        LOGGER.info( "Finished waiting for job {} {}",
                                     jobId, whichOutput );
                    }
                }
            }
            catch ( InterruptedException ie )
            {
                LOGGER.warn( "Interrupted while getting output for job {}", jobId );
                Thread.currentThread().interrupt();
            }
            catch ( IOException ioe )
            {
                // Since we may or may not actually consume result, log exception here
                LOGGER.warn( "When attempting to get job results message using {}:",
                             this, ioe );
                throw ioe;
            }


            return sharedList;
        }

        @Override
        public String toString()
        {
            StringJoiner result = new StringJoiner( ", ", "OutputWatcher with ", "" );
            result.add( "connection=" + this.getConnection() );
            result.add( "jobStatusExchangeName=" + this.getJobStatusExchangeName() );
            result.add( "jobId=" + this.getJobId() );
            return result.toString();
        }
    }

    /**
     * Register a job request id with JobResults so that results are able to
     * be completely retrieved from a back-end web service.
     * The bad part of this setup is the assumption of a single web server
     * running also holding the results in memory. The memory issue can be fixed
     * with a temp file, but really it is the single web server issue that is
     * the bigger problem. Storing results in a database may be better.
     * @param jobStatusExchangeName the queue with job results
     * @param jobId the job to look for
     * @return a future with the job id, can be ignored because results are
     * available via JobResults.getJobResult(...)
     * @throws IOException when connecting to broker fails
     * @throws TimeoutException when connecting to broker fails
     */

    Future<Integer> registerjobId( String jobStatusExchangeName,
                                           String jobId )
            throws IOException, TimeoutException
    {
        if ( JOB_RESULTS_BY_ID.putIfAbsent( jobId, JOB_NOT_DONE_YET ) != null )
        {
            LOGGER.warn( "jobId {} may have been registered twice",
                         jobId );
        }

        ResultWatcher resultWatcher = new ResultWatcher( this.getConnection(),
                                                         jobStatusExchangeName,
                                                         jobId );
        OutputWatcher stdoutWatcher = new OutputWatcher( this.getConnection(),
                                                         jobStatusExchangeName,
                                                         jobId,
                                                         OutputWatcher.WhichOutput.STDOUT );
        OutputWatcher stderrWatcher = new OutputWatcher( this.getConnection(),
                                                         jobStatusExchangeName,
                                                         jobId,
                                                         OutputWatcher.WhichOutput.STDERR );

        EXECUTOR.submit( stdoutWatcher );
        EXECUTOR.submit( stderrWatcher );
        return EXECUTOR.submit( resultWatcher );
    }


    /**
     * Get a text description of the result of a wres job
     * @param correlationId the job to look for
     * @return description of the result
     */

    static String getJobResult( String correlationId )
    {
        Integer result = JOB_RESULTS_BY_ID.get( correlationId );

        if ( result == null )
        {
            return "No job id '" + correlationId + "' was registered.";
        }
        else if ( result.equals( JOB_NOT_DONE_YET ) )
        {
            return "Job id '" + correlationId + "' still in progress.";
        }
        else
        {
            return "Job id '" + correlationId + "' finished with exit code "
                   + result;
        }
    }


    /**
     * Get the plain text of standard out for a given wres job
     * @param jobId the job to look for
     * @return the standard out from the job
     */

    static String getJobStdout( String jobId )
    {
        List<String> stdout = JOB_STDOUT_BY_ID.get( jobId );

        if ( stdout == null )
        {
            return "No job id '" + jobId + "' found.'";
        }

        StringJoiner result = new StringJoiner( System.lineSeparator() );

        for ( String s : stdout )
        {
            result.add( s );
        }

        return result.toString();
    }


    /**
     * Get the plain text of standard err for a given wres job
     * @param jobId the job to look for
     * @return the standard err from the job
     */

    static String getJobStderr( String jobId )
    {
        List<String> stderr = JOB_STDERR_BY_ID.get( jobId );

        if ( stderr == null )
        {
            return "No job id '" + jobId + "' found.'";
        }

        StringJoiner result = new StringJoiner( System.lineSeparator() );

        for ( String s : stderr )
        {
            result.add( s );
        }

        return result.toString();
    }
}
