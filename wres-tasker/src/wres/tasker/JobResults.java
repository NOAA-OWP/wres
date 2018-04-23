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
            StringJoiner result = new StringJoiner( ", ", "JobResults with ", "" );
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
