package wres.tasker;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.stream.Stream;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.protobuf.GeneratedMessageV3;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.StreamingOutput;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.eclipse.jetty.http.HttpStatus;
import org.redisson.api.RLiveObjectService;
import org.redisson.api.RMapCache;
import org.redisson.api.RedissonClient;
import org.redisson.api.map.event.EntryEvent;
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

    /** The length of time a JobMetadata "lives" before expiring from the persister. */
    private static final long EXPIRY_IN_MINUTES = Duration.ofDays( 14 )
                                                          .toMinutes();

    /** To generate job ids. The job id is a kind of token: use SecureRandom */
    private static final Random RANDOM = new SecureRandom();

    public enum WhichStream
    {
        STDOUT,
        STDERR
    }

    /** A client for redis, optional */
    private final RedissonClient redisson;

    /** The live object service from redisson when redisson is present */
    private final RLiveObjectService objectService;

    /** A shared map of job metadata by ID */
    private final ConcurrentMap<String, JobMetadata> jobMetadataById;


    /**
     * How many job results to look for at once (should probably be at least as
     * many as the count of workers active on the platform and at most the max
     * count of jobs expected to be in the job queue at any given time?)
     * TODO: limit the number of necessary threads to a fixed number, requires
     * different approach to listening for messages.
     */
    private final int NUMBER_OF_THREADS = 400;

    /**
     * An executor service that consumes job results and stores them in JOB_RESULTS_BY_ID.
     * Initialized in the constructor.
     */
    private final ExecutorService executor;

    /** The factory to get connections from, configured to reach broker */
    private final ConnectionFactory connectionFactory;

    /** The connection to use for retrieving results, long lived */
    private Connection connection;

    /** The lock to guard connection when init fails on construction */
    private final Object CONNECTION_LOCK = new Object();

    private static final String TMP_DRIVE = System.getProperty( "java.io.tmpdir" );

    private static final double DEFAULT_THRESHOLD_DISK_SPACE = 90.0;

    JobResults( ConnectionFactory connectionFactory,
                RedissonClient redissonClient )
    {
        this.connectionFactory = connectionFactory;
        // Will lazily initialize connection since trying here first requires
        // retry later anyway.
        this.connection = null;
        this.redisson = redissonClient;


        UncaughtExceptionHandler handler = ( a, b ) -> {
            String message = "Encountered an internal error while watching for job information.";
            LOGGER.warn( message, b );
        };
        ThreadFactory threadFactory = new BasicThreadFactory.Builder().namingPattern( "JobResults Thread %d" )
                                                                      .uncaughtExceptionHandler( handler )
                                                                      .build();


        //We were encountering run time exceptions in the status watcher that were not being logged. Apparently,
        //one of the wrapping classes established to run the watcher was resulting in the exception be caught,
        //but not logged.  The below is the recommended solution to see that exception. 
        //See ticket #112063 for the stack overflow from where this code was copied and reformatted.
        executor = new ThreadPoolExecutor( NUMBER_OF_THREADS,
                                           NUMBER_OF_THREADS,
                                           0L,
                                           TimeUnit.MILLISECONDS,
                                           new LinkedBlockingQueue<Runnable>(),
                                           threadFactory )
        {
            @Override
            protected void afterExecute( Runnable r, Throwable t )
            {
                super.afterExecute( r, t );
                if ( t == null && r instanceof Future<?> )
                {
                    try
                    {
                        Future<?> future = ( Future<?> ) r;
                        if ( future.isDone() )
                        {
                            future.get();
                        }
                    }
                    catch ( CancellationException ce )
                    {
                        t = ce;
                    }
                    catch ( ExecutionException ee )
                    {
                        t = ee.getCause();
                    }
                    catch ( InterruptedException ie )
                    {
                        Thread.currentThread().interrupt();
                    }
                }
                if ( t != null )
                {
                    LOGGER.warn( "A throwable was received while watching for messages in a queue.",
                                 t );
                }
            }
        };

        // Use redis when available, otherwise local Caffeine instances.
        // Set up Redisson.
        if ( Objects.nonNull( this.redisson ) )
        {
            RMapCache<String, JobMetadata> redissonMap = this.redisson.getMapCache( "jobMetadataById" );
            this.objectService = this.redisson.getLiveObjectService();
            this.objectService.registerClass( JobMetadata.class );

            // Listen for expiration of values within the map to expire metadata
            redissonMap.addListener( new EntryExpiredListener()
            {
                @Override
                public void onExpired( EntryEvent event )
                {
                    LOGGER.info( "The persister is expiring the metadata associated with job id {}. Deleting the "
                                 + "metadata and all of its fields.",
                                 event.getKey() );
                    objectService.delete( objectService.get( JobMetadata.class, event.getKey() ) );
                }
            } );

            this.jobMetadataById = redissonMap;
        }
        // Set up Caffeine.
        else
        {
            Cache<String, JobMetadata> caffeineCache = Caffeine.newBuilder()
                                                               .softValues()
                                                               .build();
            this.jobMetadataById = caffeineCache.asMap();
            this.objectService = null;
        }

        //Scan the job metadata map for jobs that are IN_QUEUE or IN_PROGRESS.  Set watchers for
        //each such job.
        for ( Map.Entry<String, JobMetadata> nextMetadata : this.jobMetadataById.entrySet() )
        {
            String jobId = nextMetadata.getKey();
            JobMetadata metadata = nextMetadata.getValue();

            if ( ( metadata.getJobState() == wres.tasker.JobMetadata.JobState.IN_QUEUE ) ||
                 ( metadata.getJobState() == wres.tasker.JobMetadata.JobState.IN_PROGRESS ) )
            {
                LOGGER.info( "Found job {} in the redis map, which has status {}.  Setting up watchers to watch it.",
                             jobId,
                             metadata.getJobState() );
                CountDownLatch latch;
                try
                {
                    latch = watchForJobFeedback( jobId,
                                                 getJobStatusExchangeName() );
                    boolean await = latch.await( 30, TimeUnit.SECONDS );

                    if ( !await )
                    {
                        shutdownNow();
                        LOGGER.warn( "Some Channel was not able to be created. There might be a leak" );
                        throw new IOException( "Unable to connect to broker" );
                    }
                }
                catch ( IOException | TimeoutException e )
                {
                    LOGGER.warn(
                            "Timed out while waiting for job feedback watchers to bind for job {}; aborting watching that job.",
                            jobId,
                            e );
                }
                catch ( InterruptedException e )
                {
                    LOGGER.warn( "Interrupted while waiting for job feedback watchers to bind for job {}.",
                                 jobId,
                                 e );
                }
            }
        }
    }

    public int getJobStatusCount( JobMetadata.JobState jobState )
    {
        int count = 0;
        for ( Map.Entry<String, JobMetadata> nextMetadata : this.jobMetadataById.entrySet() )
        {
            if ( nextMetadata.getValue().getJobState() == jobState )
            {
                count++;
            }
        }
        return count;
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

        /** Latch to report that watching/binding/listening has begun */
        private final CountDownLatch countDownLatch;

        /**
         * @param connection shared connection
         * @param jobStatusExchangeName the exchange name to look in
         * @param jobMetadata the job to look for and where to put results.
         * @param countDownLatch A latch to countdown when actually listening.
         */

        JobResultWatcher( Connection connection,
                          String jobStatusExchangeName,
                          JobMetadata jobMetadata,
                          CountDownLatch countDownLatch )
        {
            Objects.requireNonNull( connection );
            Objects.requireNonNull( jobStatusExchangeName );
            Objects.requireNonNull( jobMetadata );
            this.connection = connection;
            this.jobStatusExchangeName = jobStatusExchangeName;
            this.jobMetadata = jobMetadata;
            this.countDownLatch = countDownLatch;
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

        private CountDownLatch getCountDownLatch()
        {
            return this.countDownLatch;
        }

        /**
         * Watch for the job exit code message. After one is seen:
         * Update the job state.
         * Delete any input data associated with the job.
         *
         * @return the result of the job id (correlation id) or Integer.MIN_VALUE when interrupted
         * @throws IOException when queue declaration fails
         */

        public Integer call() throws IOException
        {
            LOGGER.debug( "call called on {}", this );
            String jobId = this.getJobId();

            BlockingQueue<Integer> result = new ArrayBlockingQueue<>( 1 );

            int resultValue = Integer.MIN_VALUE;
            String exchangeName = this.getJobStatusExchangeName();
            String exchangeType = "topic";
            String bindingKey = "job." + jobId + ".EXITCODE";
            String queueName = null;
            Channel channel = null;

            try
            {
                channel = this.getConnection().createChannel();
                if ( Objects.isNull( channel ) )
                {
                    LOGGER.warn( "Channel was unable to be created. There might be a leak" );
                    throw new IOException( "Unable to connect to broker" );
                }

                channel.exchangeDeclare( exchangeName, exchangeType, true );

                // As the consumer, I want an exclusive queue for me?
                queueName = channel.queueDeclare( bindingKey, true, false, false, null ).getQueue();

                // Does this have any effect?
                AMQP.Queue.BindOk bindResult = channel.queueBind( queueName, exchangeName, bindingKey );

                LOGGER.info( "Watching queue {} for the exit code result of the evaluation.", queueName );
                LOGGER.debug( "Bindresult: {}", bindResult );

                JobResultConsumer jobResultConsumer =
                        new JobResultConsumer( channel,
                                               result );

                String consumerTag = channel.basicConsume( queueName,
                                                           true,
                                                           jobResultConsumer );

                // Signal to other threads that we are now watching.
                this.getCountDownLatch().countDown();
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
                             jobId,
                             resultValue,
                             ie );
                Thread.currentThread().interrupt();
            }
            catch ( IOException ioe )
            {
                // Since we may or may not actually consume result, log exception here
                LOGGER.warn( "When attempting to get job results message using {}:",
                             this,
                             ioe );
                throw ioe;
            }
            finally
            {
                try
                {
                    if ( queueName != null )
                    {
                        LOGGER.info( "Deleting the queue {}", queueName );
                        AMQP.Queue.DeleteOk deleteOk = channel.queueDelete( queueName );
                        if ( deleteOk == null )
                        {
                            LOGGER.warn( "Delete queue with name {} failed. There might be a zombie queue.",
                                         queueName );
                        }
                    }

                    if ( channel != null )
                    {
                        channel.close();
                    }
                }
                catch ( IOException | TimeoutException e )
                {
                    LOGGER.warn(
                            "Delete queue with name {} failed or unable to close channel due to an exception. There might be a zombie queue.",
                            queueName,
                            e );
                }
            }

            JobMetadata sharedData = this.getJobMetadata();
            LOGGER.debug( "Shared metadata before setting exit code to {}: {}",
                          resultValue,
                          sharedData );
            sharedData.setExitCode( resultValue );
            LOGGER.debug( "Shared metadata after setting exit code to {}: {}",
                          resultValue,
                          sharedData );

            // Checking for HTTP OK status
            if ( resultValue == HttpStatus.OK_200 )
            {
                sharedData.setJobState( JobMetadata.JobState.COMPLETED_REPORTED_SUCCESS );
                LOGGER.debug( "Shared metadata after setting job state: {}",
                              jobMetadata );
                JobResults.deleteInputs( jobMetadata );
            }
            // We use 409 to signify a canceled evaluation
            else if ( resultValue == HttpStatus.CONFLICT_409 )
            {
                sharedData.setJobState( JobMetadata.JobState.CANCELED );
                LOGGER.debug( "Shared metadata after setting job state: {}",
                              jobMetadata );
                JobResults.deleteInputs( jobMetadata );
            }
            else
            {
                sharedData.setJobState( JobMetadata.JobState.COMPLETED_REPORTED_FAILURE );
                LOGGER.debug( "Shared metadata after setting job state: {}",
                              jobMetadata );
                double diskThreshold = decideThreshold();
                if ( JobResults.storageThresholdExceeded( diskThreshold ) )
                {
                    LOGGER.info( "Disk space has reached threshold... attempting to make space" );
                    List<Path> files = new ArrayList<>( JobResults.getFilesInDirectory( Path.of( TMP_DRIVE ) ) );
                    JobResults.sortFilesByLastModified( files );
                    while ( JobResults.storageThresholdExceeded( diskThreshold ) && !files.isEmpty() )
                    {
                        Path fileToDelete = files.remove( 0 );
                        JobResults.deleteFile( fileToDelete );
                        LOGGER.info( "File that was deleted: {}", fileToDelete );
                    }
                }
            }
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
     * Calculates if the storage threshold set is exceeded. The value is an arbitrary
     * value chosen and is currently set to 90% of disk space
     * @param diskThreshold which holds the either a user defined percentage threshold or the default value
     * @return A boolean value of if the space left on the disk is above the % threshold value set
     * @throws IOException When the path to the directory is not found
     */
    private static boolean storageThresholdExceeded( double diskThreshold ) throws IOException
    {
        Path path = Path.of( TMP_DRIVE );

        FileStore fileStore = Files.getFileStore( path );

        long totalSpace = fileStore.getTotalSpace();
        long usableSpace = fileStore.getUsableSpace();
        long usedSpace = totalSpace - usableSpace;


        double percentageUsed = ( ( ( double ) usedSpace / totalSpace ) * 100 );

        return percentageUsed >= diskThreshold;
    }

    /**
     * To retrieve a list of all the files in input data directory
     * @param path A string of a path to the input data path
     * @return A list of path's of all the files found in that directory
     * @throws IOException When the path to the directory is not found
     */
    public static List<Path> getFilesInDirectory( Path path ) throws IOException
    {
        try ( Stream<Path> paths = Files.list( path ) )
        {
            return paths.toList();
        }
    }

    /**
     * To sort the list of files found from the oldest modified being at
     * the start of the list to the most recent modified being at the end of the list
     * @param files The list of file paths to sort
     */
    public static void sortFilesByLastModified( List<Path> files )
    {
        files.sort( Comparator.comparingLong( path -> {
            try
            {
                return getFileLastModified( path );
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( "Error when trying to sort the files by last modified", e );
            }
        } ) );
    }

    /**
     * A utility method to be able to get the files last modified time in milliseconds
     * @param file A specific file path to retrieve its last modified time
     * @return A Long of last modified time in milliseconds
     * @throws IOException If the file path is not found
     */
    public static long getFileLastModified( Path file ) throws IOException
    {
        BasicFileAttributes attributes = Files.readAttributes( file, BasicFileAttributes.class );
        return attributes.lastModifiedTime()
                         .toMillis();
    }

    /**
     * To delete a given file path
     * @param file A path to the file wanting to be deleted
     * @throws IOException If the file path is not found
     */
    public static void deleteFile( Path file ) throws IOException
    {
        Files.deleteIfExists( file );
    }

    public static double decideThreshold()
    {
        String userThreshold = System.getProperty( "wres.dataDirectDiskThreshold" );
        if ( Objects.nonNull( userThreshold ) )
        {
            try
            {
                double provisionalThreshold = Double.parseDouble( userThreshold );
                if ( provisionalThreshold > 0.0 && provisionalThreshold < 100.0 )
                {
                    LOGGER.info( "Discovered a user-defined data direct disk threshold in the system property "
                                 + "override, 'wres.dataDirectDiskThreshold'. The threshold is: {}.",
                                 provisionalThreshold );
                    return provisionalThreshold;
                }
                else if ( LOGGER.isWarnEnabled() )
                {
                    LOGGER.warn( "Discovered an invalid 'wres.dataDirectDiskThreshold'. Expected a number that is "
                                 + "greater than 0 and less than 100, representing a percentage. but got: {}. Will "
                                 + "use the default threshold instead: {}.",
                                 provisionalThreshold,
                                 DEFAULT_THRESHOLD_DISK_SPACE );
                }
            }
            catch ( NumberFormatException e )
            {
                LOGGER.warn( "Discovered an invalid 'wres.dataDirectDiskThreshold'. Expected a number, but got: "
                             + " {}. Will use the default threshold instead: {}.",
                             userThreshold,
                             DEFAULT_THRESHOLD_DISK_SPACE );
            }
        }
        return DEFAULT_THRESHOLD_DISK_SPACE;
    }

    /**
     * Watches for the stdout|stderr of a job regardless of if anyone actually
     * is looking for it. Idea is to pro-actively seek the results of jobs
     * on the service end and cache them before the web service is called.
     */

    private static class StandardStreamWatcher implements Callable<Map<Integer, String>>
    {
        private static final int LOCAL_Q_SIZE = 10;
        private final Connection connection;
        private final String jobStatusExchangeName;
        private final JobMetadata jobMetadata;
        private final WhichStream whichStream;
        private final CountDownLatch countDownLatch;

        /**
         * @param connection shared connection
         * @param jobStatusExchangeName the exchange name to look in
         * @param jobMetadata the job to look for and where to put results
         * @param whichStream Which of the two standard streams this is.
         * @param countDownLatch A latch to countdown when actually listening.
         */
        StandardStreamWatcher( Connection connection,
                               String jobStatusExchangeName,
                               JobMetadata jobMetadata,
                               WhichStream whichStream,
                               CountDownLatch countDownLatch )
        {
            Objects.requireNonNull( connection );
            Objects.requireNonNull( jobStatusExchangeName );
            Objects.requireNonNull( jobMetadata );
            Objects.requireNonNull( whichStream );
            Objects.requireNonNull( countDownLatch );
            this.connection = connection;
            this.jobStatusExchangeName = jobStatusExchangeName;
            this.jobMetadata = jobMetadata;
            this.whichStream = whichStream;
            this.countDownLatch = countDownLatch;
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

        private CountDownLatch getCountDownLatch()
        {
            return this.countDownLatch;
        }

        /**
         * @return the stdout|stderr of the job id (correlation id) or potentially empty list when interrupted
         * @throws IOException when queue declaration fails
         */

        public Map<Integer, String> call() throws IOException, TimeoutException
        {
            String jobId = this.getJobId();
            Consumer<GeneratedMessageV3> sharer = new JobStandardStreamSharer( this.jobMetadata,
                                                                               this.getWhichStream() );
            BlockingQueue<JobStandardStream.job_standard_stream> oneLineOfOutput =
                    new ArrayBlockingQueue<>( LOCAL_Q_SIZE );

            String exchangeName = this.getJobStatusExchangeName();
            String exchangeType = "topic";
            String bindingKey = "job." + jobId
                                + "."
                                + this.getWhichStream()
                                      .name();
            String queueName = null;
            Channel channel = null;

            try
            {
                channel = this.getConnection().createChannel();
                if ( Objects.isNull( channel ) )
                {
                    LOGGER.warn( "Channel was unable to be created. There might be a leak" );
                    // We are unable to create a channel, stop waiting for one
                    this.getCountDownLatch().countDown();
                    throw new IOException( "Unable to establish connection to broker" );
                }

                channel.exchangeDeclare( exchangeName, exchangeType, true );

                // As the consumer, I want an exclusive queue for me.
                queueName = channel.queueDeclare( bindingKey, true, false, false, null ).getQueue();
                channel.queueBind( queueName, exchangeName, bindingKey );

                LOGGER.info( "Watching the queue {} for {} logging.", queueName, this.getWhichStream().toString() );

                JobStandardStreamConsumer jobStandardStreamConsumer =
                        new JobStandardStreamConsumer( channel,
                                                       oneLineOfOutput );

                channel.basicConsume( queueName,
                                      true,
                                      jobStandardStreamConsumer );
                // Signal that we are now watching.
                this.getCountDownLatch().countDown();
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
                             this,
                             ioe );
                throw ioe;
            }
            finally
            {
                try
                {
                    if ( queueName != null )
                    {
                        LOGGER.info( "Deleting the queue {}", queueName );
                        AMQP.Queue.DeleteOk deleteOk = channel.queueDelete( queueName );
                        if ( deleteOk == null )
                        {
                            LOGGER.warn( "Delete queue with name {} failed. There might be a zombie queue.",
                                         queueName );
                        }
                    }

                    if ( channel != null )
                    {
                        channel.close();
                    }
                }
                catch ( IOException | TimeoutException e )
                {
                    LOGGER.warn(
                            "Delete queue with name {} failed or unable to close channel due to an exception. There might be a zombie queue.",
                            queueName,
                            e );
                }
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
     * @param jobId The ID for which to find the metadata.
     * @return The metadata found.
     * @throws JobNotFoundException if this job is not found. That extends
     * IllegalStateException.
     */
    private JobMetadata getJobMetadataExceptIfNotFound( String jobId )
    {
        JobMetadata metadata = jobMetadataById.get( jobId );
        if ( Objects.isNull( metadata ) )
        {
            throw new JobNotFoundException( "Job id " + jobId + " not found." );
        }
        return metadata;
    }


    /**
     * Register a a new job, without yet watching for results from a worker.
     * @return The newly created and registered job id.
     */

    String registerNewJob()
    {
        // Guarantee a positive number. Using Math.abs would open up failure
        // in edge cases. A while loop seems complex. Thanks to Ted Hopp
        // on StackOverflow question id 5827023.
        long someRandomNumber = RANDOM.nextLong() & Long.MAX_VALUE;

        String jobId = String.valueOf( someRandomNumber );
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

            //Since field are discarded when making the metadata "live", ensure
            //the state is CREATED.
            jobMetadata.setJobState( JobMetadata.JobState.CREATED );

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

            metadataExisted =
                    ( ( RMapCache<String, JobMetadata> ) this.jobMetadataById )
                            .putIfAbsent( jobId,
                                          jobMetadata,
                                          EXPIRY_IN_MINUTES,
                                          TimeUnit.MINUTES ) != null;
        }
        else
        {
            metadataExisted =
                    this.jobMetadataById.putIfAbsent( jobId, jobMetadata ) != null;
        }

        if ( metadataExisted )
        {
            LOGGER.warn( "jobId {} may have been registered twice",
                         jobId );
        }

        return jobId;
    }


    /**
     * Start watching for an already-registered job's feedback via broker.
     *
     * The job must have already been registered via registerJobId.
     *
     * @param jobId The job id (already registered)
     * @param jobStatusExchangeName the exchange name for job results
     * @return A countdown latch which when at 0 indicates all watching began.
     * @throws IOException when connecting to broker fails
     * @throws TimeoutException when connecting to broker fails
     * @throws IllegalStateException when the job id is not found.
     */

    CountDownLatch watchForJobFeedback( String jobId,
                                        String jobStatusExchangeName )
            throws IOException, TimeoutException
    {
        JobMetadata jobMetadata = getJobMetadataExceptIfNotFound( jobId );

        CountDownLatch countDownLatch = new CountDownLatch( 5 );
        JobResultWatcher jobResultWatcher = new JobResultWatcher( this.getConnection(),
                                                                  jobStatusExchangeName,
                                                                  jobMetadata,
                                                                  countDownLatch );
        StandardStreamWatcher stdoutWatcher = new StandardStreamWatcher( this.getConnection(),
                                                                         jobStatusExchangeName,
                                                                         jobMetadata,
                                                                         WhichStream.STDOUT,
                                                                         countDownLatch );
        StandardStreamWatcher stderrWatcher = new StandardStreamWatcher( this.getConnection(),
                                                                         jobStatusExchangeName,
                                                                         jobMetadata,
                                                                         WhichStream.STDERR,
                                                                         countDownLatch );

        // Watch for worker reports on job status to help mark the transition
        // from JobState IN_QUEUE to IN_PROGRESS
        JobStatusWatcher jobStatusWatcher = new JobStatusWatcher( this.getConnection(),
                                                                  jobStatusExchangeName,
                                                                  jobMetadata,
                                                                  countDownLatch );

        // Share the output locations, allows service endpoint to find files.
        // Not relying on inner classes, so need to pass the shared location to
        // the watcher in order for watcher to know where to put messages.
        JobOutputWatcher jobOutputWatcher = new JobOutputWatcher( this.getConnection(),
                                                                  jobStatusExchangeName,
                                                                  jobMetadata,
                                                                  countDownLatch );

        executor.submit( stdoutWatcher );
        executor.submit( stderrWatcher );
        executor.submit( jobOutputWatcher );
        executor.submit( jobStatusWatcher );
        executor.submit( jobResultWatcher );
        return countDownLatch;
    }

    /**
     * Abruptly stops listening for job results.
     */

    void shutdownNow()
    {
        executor.shutdownNow();
    }


    /**
     * Get a description of the status of a wres evaluation job
     * @param correlationId the job id to look for
     * @return description of the status, null when not found
     */

    JobMetadata.JobState getJobState( String correlationId )
    {
        JobMetadata jobMetadata = jobMetadataById.get( correlationId );

        LOGGER.debug( "Here is the job metadata: {}", jobMetadata );

        if ( Objects.isNull( jobMetadata ) )
        {
            return JobMetadata.JobState.NOT_FOUND;
        }

        return jobMetadata.getJobState();
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
            try ( OutputStreamWriter outputStreamWriter = new OutputStreamWriter( output, StandardCharsets.UTF_8 );
                  BufferedWriter writer = new BufferedWriter( outputStreamWriter ) )
            {

                if ( jobMetadata == null )
                {
                    writer.write( "No job id '" + jobId + "' found.'" );
                    return;
                }

                Map<Integer, String> stdout = jobMetadata.getStdout();

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

        Map<Integer, String> stderr = metadata.getStderr();
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

        return metadata.retrieveOutputURIs();
    }

    /**
     * Set the declaration for a job. Can only be done once.
     * @throws IllegalStateException When job id non-existent or dec already set
     */
    void setJobMessage( String jobId, byte[] jobMessage )
    {
        LOGGER.debug( "Setting declaration for jobId={} to: \n{}",
                      jobId,
                      jobMessage );
        JobMetadata metadata = getJobMetadataExceptIfNotFound( jobId );

        byte[] originalMessage = metadata.getJobMessage();

        if ( Objects.nonNull( originalMessage ) )
        {
            throw new IllegalStateException( "Job id " + jobId
                                             + " already had declaration set!" );
        }

        metadata.setJobMessage( jobMessage );
    }

    /**
     * Get the declaration for a job
     */
    byte[] getJobMessage( String jobId )
    {
        JobMetadata metadata = getJobMetadataExceptIfNotFound( jobId );

        return metadata.getJobMessage();
    }

    /**
     * Set the name of the database for the given job.
     */
    void setDatabaseName( String jobId, String databaseName )
    {
        LOGGER.debug( "Setting databaseName for jobId={} to: {}.", jobId, databaseName );
        JobMetadata metadata = getJobMetadataExceptIfNotFound( jobId );
        String originalValue = metadata.getDatabaseName();
        if ( Objects.nonNull( originalValue ) )
        {
            throw new IllegalStateException( "Job id " + jobId
                                             + " already had database name set,"
                                             + originalValue
                                             + "!" );
        }
        metadata.setDatabaseName( databaseName );
    }

    /**
     * Get the name of the database for the given job.
     */
    String getDatabaseName( String jobId )
    {
        JobMetadata metadata = getJobMetadataExceptIfNotFound( jobId );
        return metadata.getDatabaseName();
    }

    /**
     * Set the host of the database for the given job.
     */
    void setDatabaseHost( String jobId, String databaseHost )
    {
        LOGGER.debug( "Setting databaseHost for jobId={} to: {}.", jobId, databaseHost );
        JobMetadata metadata = getJobMetadataExceptIfNotFound( jobId );
        String originalValue = metadata.getDatabaseHost();
        if ( Objects.nonNull( originalValue ) )
        {
            throw new IllegalStateException( "Job id " + jobId
                                             + " already had database host set!" );
        }
        metadata.setDatabaseHost( databaseHost );
    }

    /**
     * Get the host of the database for the given job.
     */
    String getDatabaseHost( String jobId )
    {
        JobMetadata metadata = getJobMetadataExceptIfNotFound( jobId );
        return metadata.getDatabaseHost();
    }

    /**
     * Set the port of the database for the given job.
     */
    void setDatabasePort( String jobId, String databasePort )
    {
        LOGGER.debug( "Setting databaseHost for jobId={} to: {}.", jobId, databasePort );
        JobMetadata metadata = getJobMetadataExceptIfNotFound( jobId );
        String originalValue = metadata.getDatabasePort();
        if ( Objects.nonNull( originalValue ) )
        {
            throw new IllegalStateException( "Job id " + jobId
                                             + " already had database port set!" );
        }
        metadata.setDatabaseHost( databasePort );
    }

    /**
     * Get the port of the database for the given job.
     */
    String getDatabasePort( String jobId )
    {
        JobMetadata metadata = getJobMetadataExceptIfNotFound( jobId );
        return metadata.getDatabasePort();
    }

    List<URI> getLeftInputs( String jobId )
    {
        JobMetadata metadata = getJobMetadataExceptIfNotFound( jobId );

        return Collections.unmodifiableList( metadata.retrieveLeftInputURIs() );
    }

    List<URI> getRightInputs( String jobId )
    {
        JobMetadata metadata = getJobMetadataExceptIfNotFound( jobId );

        return Collections.unmodifiableList( metadata.retrieveRightInputURIs() );
    }

    List<URI> getBaselineInputs( String jobId )
    {
        JobMetadata metadata = getJobMetadataExceptIfNotFound( jobId );

        return Collections.unmodifiableList( metadata.retrieveBaselineInputURIs() );
    }

    void addInput( String jobId, String side, URI input )
    {
        JobMetadata metadata = getJobMetadataExceptIfNotFound( jobId );

        if ( side.equalsIgnoreCase( "left" ) )
        {
            metadata.addLeftInput( input );
        }
        else if ( side.equalsIgnoreCase( "right" ) )
        {
            metadata.addRightInput( input );
        }
        else if ( side.equalsIgnoreCase( "baseline" ) )
        {
            metadata.addBaselineInput( input );
        }
        else
        {
            throw new UnsupportedOperationException( "Unsupported side " + side );
        }
    }


    /**
     * Delete outputs for the give job id.
     * @param jobId The job id.
     * @throws JobNotFoundException if the job id is not found. Make sure
     * to catch this before the lower IllegalStateException!
     * @throws IOException If a file delete throws the exception.
     * @throws IllegalStateException if a file delete throws the exception
     */
    void deleteOutputs( String jobId ) throws JobNotFoundException, IOException
    {
        JobMetadata metadata = getJobMetadataExceptIfNotFound( jobId );
        deleteOutputs( metadata );
    }

    /**
     * Delete the outputs contained in the specified shared data.
     * The metadata will be updated with successfully deleted files, 
     * even if an exception occurs while deleting the files. In that
     * case, the metadata is updated by deletion halts and the
     * exception is propagated to the caller. 
     * @param sharedData The metadata with the output to delete.
     * @throws IOException If a file delete throws the exception.
     * @throws IllegalStateException if a file delete throws the exception.
     */
    void deleteOutputs( JobMetadata sharedData ) throws IOException
    {
        Set<URI> jobOutputs = sharedData.retrieveOutputURIs();
        Set<URI> deletedOutputs = new ConcurrentSkipListSet<>();

        //Remove each file if it exists. Log messages appropriately.
        //All exceptions are propagated to the caller.
        try
        {
            for ( URI outputResource : jobOutputs )
            {
                java.nio.file.Path path = Paths.get( outputResource.getPath() );

                boolean deleted = Files.deleteIfExists( path );
                if ( !deleted )
                {
                    LOGGER.warn( "Failed to delete {} because the file does not exist."
                                 + " Was it previously deleted?",
                                 path );
                }
                else
                {
                    LOGGER.debug( "Deleted {}", path );
                }

                //If we reach this point, then the file was either deleted
                //or did not exist. Regardless, it should not be in the 
                //metadata, so add it to the list of files to be removed
                //from the metadata.
                deletedOutputs.add( outputResource );
            }
        }
        //Inside of a finally, I want to make sure the metadata
        //reflects the deletions that succeeded. 
        finally
        {
            sharedData.removeOutputs( deletedOutputs );
        }
    }

    /**
     * Delete inputs associated with the given job id. This instance method
     * is here so that it can be called from WresJobInput.
     * @param jobId The job id.
     * @throws JobNotFoundException if the job id is not found.
     */
    void deleteInputs( String jobId )
    {
        JobMetadata sharedMetadata = getJobMetadataExceptIfNotFound( jobId );
        JobResults.deleteInputs( sharedMetadata );
    }

    /**
     * Delete inputs associated with the given job id. This instance method
     * is here so that it can be called from WresJobInput.
     * @param jobId The job id.
     * @throws JobNotFoundException if the job id is not found.
     */
    void deleteInputs( String jobId, boolean force )
    {
        JobMetadata sharedMetadata = getJobMetadataExceptIfNotFound( jobId );
        JobResults.deleteInputs( sharedMetadata, force );
    }

    /**
     * Delete any posted data associated with the given job metadata.
     * This one is static such that it can be called by JobResultWatcher.
     * @param sharedData the JobMetadata associated for the job. 
     */
    private static void deleteInputs( JobMetadata sharedData )
    {
        deleteInputs( sharedData, false );
    }

    /**
     * Delete any posted data associated with the given job metadata.
     * This will not clear the lists in the JobMetadata, itself, since
     * those do not need to be presented to the user. This is not the
     * same behavior as job outputs which requires a separate remove 
     * method for the metadata since the user will see a list of outputs.
     * Exceptions lead to warnings, not errors.
     * @param sharedData the JobMetadata associated for the job.
     * @param force Set to true to force the delete regardless of keep flag.
     */
    private static void deleteInputs( JobMetadata sharedData, boolean force )
    {
        if ( sharedData.getKeepInput() && !force )
        {
            return;
        }
        String jobId = sharedData.getId();

        // When there are posted input data related to this job, remove them
        for ( URI uri : sharedData.retrieveLeftInputURIs() )
        {
            try
            {
                Path pathToDelete = Paths.get( uri );
                Files.deleteIfExists( pathToDelete );
            }
            catch ( IOException ioe )
            {
                LOGGER.warn( "Failed to delete left data for job {} at {}",
                             jobId,
                             uri,
                             ioe );
            }
        }
        for ( URI uri : sharedData.retrieveRightInputURIs() )
        {
            try
            {
                Path pathToDelete = Paths.get( uri );
                Files.deleteIfExists( pathToDelete );
            }
            catch ( IOException ioe )
            {
                LOGGER.warn( "Failed to delete right data for job {} at {}",
                             jobId,
                             uri,
                             ioe );
            }
        }

        for ( URI uri : sharedData.retrieveBaselineInputURIs() )
        {
            try
            {
                Path pathToDelete = Paths.get( uri );
                Files.deleteIfExists( pathToDelete );
            }
            catch ( IOException ioe )
            {
                LOGGER.warn( "Failed to delete baseline data for job {} at {}",
                             jobId,
                             uri,
                             ioe );
            }
        }
    }

    /**
     * Mark the job as being in the queue, transition state to IN_QUEUE
     *
     * Tolerates an illegal transition because of known race condition where the
     * job might have finished before we mark it as being IN_QUEUE.
     * @param jobId The job to mark as being IN_QUEUE.
     * @throws IllegalArgumentException When job not found.
     */
    void setInQueue( String jobId )
    {
        JobMetadata metadata = getJobMetadataExceptIfNotFound( jobId );

        try
        {
            metadata.setJobState( JobMetadata.JobState.IN_QUEUE );
        }
        catch ( IllegalStateException ise )
        {
            if ( ise.getMessage()
                    .contains( JobMetadata.CAN_ONLY_TRANSITION_FROM ) )
            {
                LOGGER.warn( "Job may have finished very quickly, already in a newer/later state:", ise );
            }
            else
            {
                // Rethrow if this is some other IllegalStateException.
                throw ise;
            }
        }
    }


    /**
     * Mark the job as awaiting input data.
     * @param jobId The job to mark as waiting for input data to be posted.
     * @throws IllegalArgumentException When job not found.
     * @throws IllegalStateException When illegal state transition is requested.
     */

    void setAwaitingPostInputData( String jobId )
    {
        JobMetadata metadata = getJobMetadataExceptIfNotFound( jobId );

        metadata.setJobState( JobMetadata.JobState.AWAITING_POSTS_OF_DATA );
    }


    /**
     * Mark the job as no longer accepting input data, transition state to some
     * other thing besides AWAITING_DATA.
     * @param jobId The job to mark as no longer accepting input data.
     * @throws IllegalArgumentException When job not found.
     * @throws IllegalStateException When illegal state transition is requested.
     */

    void setPostInputDone( String jobId )
    {
        JobMetadata metadata = getJobMetadataExceptIfNotFound( jobId );

        metadata.setJobState( JobMetadata.JobState.NO_MORE_POSTS_OF_DATA );
    }


    /**
     * Mark the job as failed before it made it into the queue.
     * @param jobId The job to mark as failed.
     * @throws IllegalArgumentException When job not found.
     * @throws IllegalStateException When illegal state transition is requested.
     */

    void setFailedBeforeInQueue( String jobId )
    {
        JobMetadata metadata = getJobMetadataExceptIfNotFound( jobId );

        metadata.setJobState( JobMetadata.JobState.FAILED_BEFORE_IN_QUEUE );
    }

    /**
     * Mark whether the inputs should be kept after the evaluation
     * completes.
     * @param keepInput True to keep input, false if not.
     */
    void setKeepPostedInputData( String jobId, boolean keepInput )
    {
        JobMetadata metadata = getJobMetadataExceptIfNotFound( jobId );

        metadata.setKeepInput( keepInput );
    }
}


