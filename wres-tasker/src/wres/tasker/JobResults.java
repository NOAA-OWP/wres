package wres.tasker;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.StringJoiner;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.protobuf.GeneratedMessageV3;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import jakarta.ws.rs.core.StreamingOutput;
import org.redisson.api.RLiveObjectService;
import org.redisson.api.RMapCache;
import org.redisson.api.RedissonClient;
import org.redisson.api.map.event.EntryExpiredListener;
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
    private static final long EXPIRY_IN_MINUTES = Duration.ofDays( 14 )
                                                          .toMinutes();

    public enum WhichStream
    {
        STDOUT,
        STDERR;
    }

    /** A client for redis, optional */
    private final RedissonClient redisson;

    /** The live object service from redisson when redisson is present */
    private final RLiveObjectService objectService;

    /** A shared map of job metadata by ID */
    private final ConcurrentMap<String,JobMetadata> jobMetadataById;


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

    JobResults( ConnectionFactory connectionFactory,
                RedissonClient redissonClient )
    {
        this.connectionFactory = connectionFactory;
        // Will lazily initialize connection since trying here first requires
        // retry later anyway.
        this.connection = null;
        this.redisson = redissonClient;

        // Use redis when available, otherwise local Caffeine instances.
        if ( Objects.nonNull( this.redisson ) )
        {
            RMapCache<String,JobMetadata> redissonMap = this.redisson.getMapCache( "jobMetadataById" );
            this.objectService = this.redisson.getLiveObjectService();
            this.objectService.registerClass( JobMetadata.class );

            // Listen for expiration of values within the map to expire metadata
            redissonMap.addListener(
                    ( EntryExpiredListener<?,?> ) event
                            -> this.objectService.delete( JobMetadata.class, event.getKey() ) );
            this.jobMetadataById = redissonMap;
        }
        else
        {
            Cache<String,JobMetadata> caffeineCache = Caffeine.newBuilder()
                                                              .softValues()
                                                              .build();
            this.jobMetadataById = caffeineCache.asMap();
            this.objectService = null;
        }
    }

    private boolean usingRedis()
    {
        return Objects.nonNull( this.redisson );
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

    private RLiveObjectService getObjectService()
    {
        return this.objectService;
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
        private final JobMetadata jobMetadata;

        /**
         * @param connection shared connection
         * @param jobStatusExchangeName the exchange name to look in
         * @param jobMetadata the job to look for and where to put results.
         */

        JobResultWatcher( Connection connection,
                          String jobStatusExchangeName,
                          JobMetadata jobMetadata )
        {
            Objects.requireNonNull( connection );
            Objects.requireNonNull( jobStatusExchangeName );
            Objects.requireNonNull( jobMetadata );
            this.connection = connection;
            this.jobStatusExchangeName = jobStatusExchangeName;
            this.jobMetadata = jobMetadata;
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
            return this.jobMetadata.getId();
        }

        private JobMetadata getJobMetadata()
        {
            return this.jobMetadata;
        }

        /**
         * @return the result of the job id (correlation id) or Integer.MIN_VALUE when interrupted
         * @throws IOException when queue declaration fails
         */

        public Integer call() throws IOException, TimeoutException
        {
            LOGGER.debug( "call called on {}", this );
            String jobId = this.getJobId();

            BlockingQueue<Integer> result = new ArrayBlockingQueue<>( 1 );

            int resultValue = Integer.MIN_VALUE;
            String exchangeName = this.getJobStatusExchangeName();
            String exchangeType = "topic";
            String bindingKey = "job." + jobId + ".exitCode";

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

            JobMetadata sharedData = this.getJobMetadata();
            LOGGER.debug( "Shared metadata before setting exit code to {}: {}",
                          resultValue, sharedData );
            sharedData.setExitCode( resultValue );
            LOGGER.debug( "Shared metadata after setting exit code to {}: {}",
                          resultValue, sharedData );

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

    private static class StandardStreamWatcher implements Callable<Map<Integer,String>>
    {
        private static final int LOCAL_Q_SIZE = 10;
        private final Connection connection;
        private final String jobStatusExchangeName;
        private final JobMetadata jobMetadata;
        private final WhichStream whichStream;

        /**
         * @param connection shared connection
         * @param jobStatusExchangeName the exchange name to look in
         * @param jobMetadata the job to look for and where to put results
         * @param whichStream Which of the two standard streams this is.
         */
        StandardStreamWatcher( Connection connection,
                               String jobStatusExchangeName,
                               JobMetadata jobMetadata,
                               WhichStream whichStream )
        {
            Objects.requireNonNull( connection );
            Objects.requireNonNull( jobStatusExchangeName );
            Objects.requireNonNull( jobMetadata );
            Objects.requireNonNull( whichStream );
            this.connection = connection;
            this.jobStatusExchangeName = jobStatusExchangeName;
            this.jobMetadata = jobMetadata;
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
            return this.jobMetadata.getId();
        }

        private WhichStream getWhichStream()
        {
            return this.whichStream;
        }

        /**
         * @return the stdout|stderr of the job id (correlation id) or potentially empty list when interrupted
         * @throws IOException when queue declaration fails
         */

        public Map<Integer,String> call() throws IOException, TimeoutException
        {
            String jobId = this.getJobId();
            Consumer<GeneratedMessageV3> sharer = new JobStandardStreamSharer( this.jobMetadata,
                                                                               this.getWhichStream() );
            BlockingQueue<JobStandardStream.job_standard_stream> oneLineOfOutput
                    = new ArrayBlockingQueue<>( LOCAL_Q_SIZE );

            String exchangeName = this.getJobStatusExchangeName();
            String exchangeType = "topic";
            String bindingKey = "job." + jobId + "." + this.getWhichStream()
                                                           .name();

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

            if ( this.getWhichStream()
                     .equals( WhichStream.STDOUT ) )
            {
                return this.jobMetadata.getStdout();
            }
            else if ( this.getWhichStream()
                          .equals( WhichStream.STDERR ) )
            {
                return this.jobMetadata.getStderr();
            }
            else
            {
                throw new IllegalStateException( "Stream must be either "
                                                 + WhichStream.STDERR.name()
                                                 + " or "
                                                 + WhichStream.STDOUT.name() );
            }
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
        JobMetadata jobMetadata = new JobMetadata( jobId );
        boolean metadataExisted;

        if ( this.usingRedis() )
        {
            // Register the object with redisson (and redis, underneath).
            // The returned object is the Live Object, or proxy, so when this
            // reference is used within this JVM, the state is actually shared
            // in redis. Therefore it can be persisted by redis. Therefore it
            // can be shared between JVMs someday, if we want, as if on a shared
            // heap between JVMs.
            jobMetadata = this.getObjectService()
                              .attach( jobMetadata );

            /* For whatever reason, the following simply does not work. Sigh.
               Instead, see the listener above attached to the map when created.
            // RLiveObject instances are RExpirable, set expiration both on the
            // instance we just created, and also its reference in the RMapCache
            boolean expiring = ( ( RExpirable ) jobMetadata ).expire( EXPIRY_IN_MINUTES, TimeUnit.MINUTES );

            if ( !expiring )
            {
                LOGGER.warn( "Unexpectedly unable to expire jobMetadata instance {}",
                             jobMetadata );
            }
             */

            metadataExisted = ( ( RMapCache<String,JobMetadata> ) this.jobMetadataById )
                    .putIfAbsent( jobId, jobMetadata,
                                  EXPIRY_IN_MINUTES, TimeUnit.MINUTES ) != null;
        }
        else
        {
            metadataExisted = this.jobMetadataById.putIfAbsent( jobId, jobMetadata ) != null;
        }

        if ( metadataExisted )
        {
            LOGGER.warn( "jobId {} may have been registered twice",
                         jobId );
        }

        JobResultWatcher jobResultWatcher = new JobResultWatcher( this.getConnection(),
                                                                  jobStatusExchangeName,
                                                                  jobMetadata );
        StandardStreamWatcher stdoutWatcher = new StandardStreamWatcher( this.getConnection(),
                                                                         jobStatusExchangeName,
                                                                         jobMetadata,
                                                                         WhichStream.STDOUT );
        StandardStreamWatcher stderrWatcher = new StandardStreamWatcher( this.getConnection(),
                                                                         jobStatusExchangeName,
                                                                         jobMetadata,
                                                                         WhichStream.STDERR );

        // Share the output locations, allows service endpoint to find files.
        // Not relying on inner classes, so need to pass the shared location to
        // the watcher in order for watcher to know where to put messages.
        JobOutputWatcher jobOutputWatcher = new JobOutputWatcher( this.getConnection(),
                                                                  jobStatusExchangeName,
                                                                  jobMetadata );

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

    JobState getJobResult( String correlationId )
    {
        JobMetadata result = jobMetadataById.get( correlationId );

        LOGGER.debug( "Here is the job result: {}", result );

        if ( Objects.isNull( result ) )
        {
            return JobState.NOT_FOUND;
        }
        else if ( !result.isFinished() )
        {
            return JobState.IN_PROGRESS;
        }
        else if ( result.getExitCode() == 0 )
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

    Integer getJobResultRaw( String correlationId )
    {
        JobMetadata metadata = jobMetadataById.get( correlationId );

        LOGGER.debug( "Here is the job metadata: {}", metadata );

        if ( Objects.isNull( metadata ) )
        {
            return null;
        }
        else if ( !metadata.isFinished() )
        {
            return JOB_NOT_DONE_YET;
        }
        else
        {
            return metadata.getExitCode();
        }
    }


    /**
     * Get the plain text of standard out for a given wres job
     * @param jobId the job to look for
     * @return A StreamingOutput having standard out
     */

    StreamingOutput getJobStdout( String jobId )
    {
        JobMetadata jobMetadata = jobMetadataById.get( jobId );

        StreamingOutput streamingOutput = output -> {
            try ( OutputStreamWriter outputStreamWriter =  new OutputStreamWriter( output );
                  BufferedWriter writer = new BufferedWriter( outputStreamWriter ) )
            {

                if ( jobMetadata == null )
                {
                    writer.write( "No job id '" + jobId + "' found.'" );
                    return;
                }

                Map<Integer,String> stdout = jobMetadata.getStdout();

                // There is an assumption that the worker starts counting at 0
                int previousIndex = -1;
                SortedSet<Integer> sortedKeys = new TreeSet<>( stdout.keySet() );

                for ( Integer index : sortedKeys )
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

    String getJobStderr( String jobId )
    {
        JobMetadata metadata = jobMetadataById.get( jobId );

        if ( Objects.isNull( metadata ) )
        {
            return "No job id '" + jobId + "' found.'";
        }

        Map<Integer,String> stderr = metadata.getStderr();
        StringJoiner result = new StringJoiner( System.lineSeparator() );
        SortedSet<Integer> sortedKeys = new TreeSet<>( stderr.keySet() );

        for ( Integer index : sortedKeys )
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

    Set<URI> getJobOutputs( String jobId )
    {
        JobMetadata metadata = jobMetadataById.get( jobId );

        if ( Objects.isNull( metadata ) )
        {
            return null;
        }

        return metadata.getOutputs();
    }
}
