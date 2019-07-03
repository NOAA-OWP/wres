package wres.tasker;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import javax.ws.rs.core.StreamingOutput;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.protobuf.GeneratedMessageV3;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static wres.tasker.JobMessageHelper.JOB_NOT_DONE_YET;

import wres.messages.generated.JobStandardStream;


/**
 * Used to get information about results and where to find them.
 * Eagerly looks in job result queue when a job id is registered, so that a
 * call from a web service will have the complete result.
 */

class JobResults
{
    private static final Logger LOGGER = LoggerFactory.getLogger( JobResults.class );

    /** A shared map of job results by ID */
    private static final Cache<String,Integer> JOB_RESULTS_BY_ID = Caffeine.newBuilder()
                                                                           .maximumSize( 500 )
                                                                           .build();

    /** A shared map of job standard out */
    private static final Cache<String, ConcurrentNavigableMap<Integer,String>> JOB_STDOUT_BY_ID
            = Caffeine.newBuilder()
                      .maximumSize( 50 )
                      .build();

    /** A shared map of job standard error */
    private static final Cache<String, ConcurrentNavigableMap<Integer,String>> JOB_STDERR_BY_ID
            = Caffeine.newBuilder()
                      .maximumSize( 50 )
                      .build();

    /** A shared map of job output references */
    private static final Cache<String, ConcurrentSkipListSet<URI>> JOB_OUTPUTS_BY_ID
            = Caffeine.newBuilder()
                      .maximumSize( 500 )
                      .build();

    /**
     * How many job results to look for at once (should probably be at least as
     * many as the count of workers active on the platform and at most the max
     * count of jobs expected to be in the job queue at any given time?)
     * TODO: limit the number of necessary threads to a fixed number, requires
     * different approach to listening for messages.
     */
    private final int NUMBER_OF_THREADS = 400;

    /** An executor service that consumes job results and stores them in JOB_RESULTS_BY_ID. */
    private final ExecutorService EXECUTOR = Executors.newFixedThreadPool( NUMBER_OF_THREADS );

    /** The factory to get connections from, configured to reach broker */
    private final ConnectionFactory connectionFactory;

    /** The connection to use for retrieving results, long lived */
    private Connection connection;

    /** The lock to guard connection when init fails on construction */
    private final Object CONNECTION_LOCK = new Object();

    /** Descriptions of the state of a WRES evaluation job */
    enum JobState
    {
        NOT_FOUND,
        // We could also have IN_QUEUE which would require better tracking
        IN_PROGRESS,
        COMPLETED_REPORTED_SUCCESS,
        COMPLETED_REPORTED_FAILURE
    }

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

    private static class JobResultWatcher implements Callable<Integer>
    {
        private final Connection connection;
        private final String jobStatusExchangeName;
        private final String jobId;

        /**
         * @param connection shared connection
         * @param jobStatusExchangeName the exchange name to look in
         * @param jobId the job identifier to look for
         */
        JobResultWatcher( Connection connection,
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

                JobResultConsumer jobResultConsumer =
                        new JobResultConsumer( channel,
                                               result );

                String consumerTag = channel.basicConsume( queueName,
                                                           true,
                                                           jobResultConsumer );
                LOGGER.debug( "consumerTag: {}", consumerTag );

                // There is a race condition between basicConsume above being
                // called and the exitCode being sent. The following message
                // may help discover some cases when the race is lost by this
                // Thread.
                LOGGER.info( "Looking for exit code on topic {}", bindingKey );

                LOGGER.debug( "Waiting to take a result value..." );
                resultValue = result.take();
                LOGGER.debug( "Finished taking a result value." );
            }
            catch ( InterruptedException ie )
            {
                LOGGER.warn( "Interrupted while getting result for job {}, returning potentially fake result {}",
                             jobId, resultValue, ie );
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
            StringJoiner result = new StringJoiner( ", ", "JobResultWatcher with ", "" );
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

    private static class StandardStreamWatcher implements Callable<ConcurrentNavigableMap<Integer,String>>
    {
        private static final int LOCAL_Q_SIZE = 10;

        public enum WhichStream
        {
            STDOUT,
            STDERR;
        }

        private final Connection connection;
        private final String jobStatusExchangeName;
        private final String jobId;
        private final WhichStream whichStream;

        /**
         * @param connection shared connection
         * @param jobStatusExchangeName the exchange name to look in
         * @param jobId the job identifier to look for
         */
        StandardStreamWatcher( Connection connection,
                               String jobStatusExchangeName,
                               String jobId,
                               WhichStream whichStream )
        {
            this.connection = connection;
            this.jobStatusExchangeName = jobStatusExchangeName;
            this.jobId = jobId;
            this.whichStream = whichStream;
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

        private WhichStream getWhichStream()
        {
            return this.whichStream;
        }

        /**
         * @return the stdout|stderr of the job id (correlation id) or potentially empty list when interrupted
         * @throws IOException when queue declaration fails
         */

        public ConcurrentNavigableMap<Integer,String> call() throws IOException, TimeoutException
        {
            ConcurrentNavigableMap<Integer,String> sharedList = new ConcurrentSkipListMap<>();
            Consumer<GeneratedMessageV3> sharer = new JobStandardStreamSharer( sharedList );

            // Store shared list to the static bag-o-state.
            if ( this.getWhichStream().equals( WhichStream.STDOUT ) )
            {
                JOB_STDOUT_BY_ID.put( this.getJobId(), sharedList );
            }
            else if ( this.getWhichStream().equals( WhichStream.STDERR ) )
            {
                JOB_STDERR_BY_ID.put( this.getJobId(), sharedList );
            }
            else
            {
                throw new IllegalStateException( "Output has to be stderr or stdout." );
            }

            BlockingQueue<JobStandardStream.job_standard_stream> oneLineOfOutput
                    = new ArrayBlockingQueue<>( LOCAL_Q_SIZE );

            String exchangeName = this.getJobStatusExchangeName();
            String exchangeType = "topic";
            String bindingKey = "job." + this.getJobId() + "." + this.getWhichStream().name();

            try ( Channel channel = this.getConnection().createChannel() )
            {
                channel.exchangeDeclare( exchangeName, exchangeType );

                // As the consumer, I want an exclusive queue for me.
                String queueName = channel.queueDeclare().getQueue();
                channel.queueBind( queueName, exchangeName, bindingKey );

                JobStandardStreamConsumer jobStandardStreamConsumer =
                        new JobStandardStreamConsumer( channel,
                                                       oneLineOfOutput );

                channel.basicConsume( queueName,
                                      true,
                                      jobStandardStreamConsumer );

                JobMessageHelper.waitForAllMessages( queueName,
                                                     this.getJobId(),
                                                     oneLineOfOutput,
                                                     sharer,
                                                     this.getWhichStream().toString() );
            }
            catch ( InterruptedException ie )
            {
                LOGGER.warn( "Interrupted while getting output for job {}", jobId, ie );
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
            StringJoiner result = new StringJoiner( ", ", "StandardStreamWatcher with ", "" );
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
        if ( JOB_RESULTS_BY_ID.asMap().putIfAbsent( jobId, JOB_NOT_DONE_YET ) != null )
        {
            LOGGER.warn( "jobId {} may have been registered twice",
                         jobId );
        }

        JobResultWatcher jobResultWatcher = new JobResultWatcher( this.getConnection(),
                                                                  jobStatusExchangeName,
                                                                  jobId );
        StandardStreamWatcher stdoutWatcher = new StandardStreamWatcher( this.getConnection(),
                                                                         jobStatusExchangeName,
                                                                         jobId,
                                                                         StandardStreamWatcher.WhichStream.STDOUT );
        StandardStreamWatcher stderrWatcher = new StandardStreamWatcher( this.getConnection(),
                                                                         jobStatusExchangeName,
                                                                         jobId,
                                                                         StandardStreamWatcher.WhichStream.STDERR );

        // Share the output locations, allows service endpoint to find files.
        // Not relying on inner classes, so need to pass the shared location to
        // the watcher in order for watcher to know where to put messages.
        ConcurrentSkipListSet<URI> outputsForOneJob = new ConcurrentSkipListSet<>();
        JOB_OUTPUTS_BY_ID.asMap()
                         .putIfAbsent( jobId, outputsForOneJob );
        JobOutputWatcher jobOutputWatcher = new JobOutputWatcher( this.getConnection(),
                                                                  jobStatusExchangeName,
                                                                  jobId,
                                                                  outputsForOneJob );

        EXECUTOR.submit( stdoutWatcher );
        EXECUTOR.submit( stderrWatcher );
        EXECUTOR.submit( jobOutputWatcher );
        return EXECUTOR.submit( jobResultWatcher );
    }

    /**
     * Abruptly stops listening for job results.
     */

    void shutdownNow()
    {
        EXECUTOR.shutdownNow();
    }


    /**
     * Get a description of the status of a wres evaluation job
     * @param correlationId the job id to look for
     * @return description of the status
     */

    static JobState getJobResult( String correlationId )
    {
        Integer result = JOB_RESULTS_BY_ID.asMap().get( correlationId );

        if ( Objects.isNull( result ) )
        {
            return JobState.NOT_FOUND;
        }
        else if ( result.equals( JOB_NOT_DONE_YET ) )
        {
            return JobState.IN_PROGRESS;
        }
        else if ( result.equals( 0 ) )
        {
            return JobState.COMPLETED_REPORTED_SUCCESS;
        }
        else
        {
            return JobState.COMPLETED_REPORTED_FAILURE;
        }
    }

    /**
     * Return the raw job result, which can be the exit code of the application,
     * or JOB_NOT_DONE_YET, or null if it was never registered.
     * @param correlationId
     * @return
     */

    static Integer getJobResultRaw( String correlationId )
    {
        return JOB_RESULTS_BY_ID.asMap()
                                .get( correlationId );
    }

    /**
     * Get the plain text of standard out for a given wres job
     * @param jobId the job to look for
     * @return A StreamingOutput having standard out
     */

    static StreamingOutput getJobStdout( String jobId )
    {
        ConcurrentNavigableMap<Integer,String> stdout = JOB_STDOUT_BY_ID.asMap()
                                                                        .get( jobId );

        StreamingOutput streamingOutput = output -> {
            try ( OutputStreamWriter outputStreamWriter =  new OutputStreamWriter( output );
                  BufferedWriter writer = new BufferedWriter( outputStreamWriter ) )
            {

                if ( stdout == null )
                {
                    writer.write( "No job id '" + jobId + "' found.'" );
                    return;
                }

                // There is an assumption that the worker starts counting at 0
                int previousIndex = -1;

                for ( Integer index : stdout.keySet() )
                {
                    int indexDiff = index - previousIndex;

                    // Handle missing lines by looking for gaps in incrementing integer.
                    if ( indexDiff > 1 )
                    {
                        writer.write( "*** Missing " + ( indexDiff - 1 ) + " lines ***" );
                        writer.newLine();
                    }

                    // The reason this is not in an "else" nor do we check for
                    // null? We are iterating over existing keys in the loop.
                    String stdoutLine = stdout.get( index );
                    writer.write( stdoutLine );
                    writer.newLine();
                    previousIndex = index;
                }
            }
        };

        return streamingOutput;
    }


    /**
     * Get the plain text of standard err for a given wres job
     * @param jobId the job to look for
     * @return the standard err from the job
     */

    static String getJobStderr( String jobId )
    {
        ConcurrentNavigableMap<Integer,String> stderr = JOB_STDERR_BY_ID.asMap()
                                                                        .get( jobId );

        if ( stderr == null )
        {
            return "No job id '" + jobId + "' found.'";
        }

        StringJoiner result = new StringJoiner( System.lineSeparator() );

        for ( Integer index : stderr.keySet() )
        {
            result.add( stderr.get( index ) );
        }

        return result.toString();
    }


    /**
     * Get the list of outputs for a job
     * @param jobId the job to look for
     * @return the flat set of output resources associated with that job or null
     * if job not found
     */

    static Set<URI> getJobOutputs( String jobId )
    {
        return JOB_OUTPUTS_BY_ID.asMap()
                                .get( jobId );
    }

}
