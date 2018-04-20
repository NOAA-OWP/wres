package wres.tasker;

import java.io.IOException;
import java.util.StringJoiner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

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
     * Get the results queue name to get results from workers from.
     * @return a results queue name to use
     */

    static String getResultsQueueName()
    {
        return "wres.jobResults";
    }


    /**
     * Watches for the result code of a job regardless of if anyone actually
     * is looking for it. Idea is to pro-actively seek the results of jobs
     * on the service end and cache them before the web service is called.
     */

    private static class ResultWatcher implements Callable<Integer>
    {
        private final Connection connection;
        private final String queueName;
        private final String correlationId;

        /**
         * @param connection shared connection
         * @param queueName the queue name to look in
         * @param correlationId the job identifier to look for
         */
        ResultWatcher( Connection connection,
                       String queueName,
                       String correlationId )
        {
            this.connection = connection;
            this.queueName = queueName;
            this.correlationId = correlationId;
            LOGGER.debug( "Instantiated {}", this );
        }

        private Connection getConnection()
        {
            return this.connection;
        }

        private String getQueueName()
        {
            return this.queueName;
        }

        private String getCorrelationId()
        {
            return this.correlationId;
        }


        /**
         * @return the result of the job id (correlation id) or Integer.MIN_VALUE when interrupted
         * @throws IOException when queue declaration fails
         */

        public Integer call() throws IOException, TimeoutException
        {
            LOGGER.debug( "call called on {}", this );

            BlockingQueue<Integer> result = new ArrayBlockingQueue<>( 1 );

            try ( Channel channel = this.getConnection().createChannel() )
            {
                channel.queueDeclare( this.getQueueName(), false, false, false, null );

                JobResultReceiver jobResultReceiver =
                        new JobResultReceiver( channel,
                                               this.getCorrelationId(),
                                               result );

                channel.basicConsume( this.getQueueName(),
                                      true,
                                      jobResultReceiver );
            }
            catch ( IOException ioe )
            {
                // Since we may or may not actually consume result, log exception here
                LOGGER.warn( "When attempting to get job results message using {}:",
                             this, ioe );
                throw ioe;
            }

            int resultValue = Integer.MIN_VALUE;

            try
            {
                resultValue = result.take();
            }
            catch ( InterruptedException ie )
            {
                LOGGER.warn( "Interrupted while getting result for job {}, returning potentially fake result {}",
                             correlationId,
                             resultValue );
                Thread.currentThread().interrupt();
            }

            // Store state back out to the static bag-o-state. A better way?
            JOB_RESULTS_BY_ID.putIfAbsent( correlationId, resultValue );

            return resultValue;
        }

        @Override
        public String toString()
        {
            StringJoiner result = new StringJoiner( ", ", "JobResults with ", "" );
            result.add( "connection=" + this.getConnection() );
            result.add( "queueName=" + this.getQueueName() );
            result.add( "correlationId=" + this.getCorrelationId() );
            return result.toString();
        }
    }


    /**
     * Register a job request id with JobResults so that results are able to
     * be completely retrieved from a back-end web service.
     * @param queueName the queue with job results
     * @param correlationId the job to look for
     * @return a future with the job id, can be ignored because results are
     * available via JobResults.getJobResult(...)
     * @throws IOException when connecting to broker fails
     * @throws TimeoutException when connecting to broker fails
     */

    Future<Integer> registerCorrelationId( String queueName,
                                           String correlationId )
            throws IOException, TimeoutException
    {
        if ( JOB_RESULTS_BY_ID.putIfAbsent( correlationId, JOB_NOT_DONE_YET ) != null )
        {
            LOGGER.warn( "Job correlationId {} may have been registered twice",
                         correlationId );
        }

        ResultWatcher resultWatcher = new ResultWatcher( this.getConnection(),
                                                         queueName,
                                                         correlationId );
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

}
