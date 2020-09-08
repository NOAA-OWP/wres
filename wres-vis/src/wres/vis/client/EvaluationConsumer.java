package wres.vis.client;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;
import java.util.function.Consumer;

import javax.jms.JMSException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DestinationType;
import wres.datamodel.MetricConstants.StatisticType;
import wres.events.GroupCompletionTracker;
import wres.events.OneGroupConsumer;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.EvaluationStatus;
import wres.statistics.generated.Statistics;
import wres.statistics.generated.EvaluationStatus.CompletionStatus;
import wres.vis.writing.GraphicsWriteException;

/**
 * Consumer of messages for one evaluation.
 * 
 * @author james.brown@hydrosolved.com
 */

class EvaluationConsumer
{
    private static final Logger LOGGER = LoggerFactory.getLogger( EvaluationConsumer.class );

    /**
     * Evaluation identifier.
     */

    private final String evaluationId;

    /**
     * Consumer identifier.
     */

    private final String consumerId;

    /**
     * Actual number of messages consumed.
     */

    private final AtomicInteger consumed;

    /**
     * Expected number of messages.
     */

    private final AtomicInteger expected;

    /**
     * Registered complete.
     */

    private final AtomicBoolean isComplete;

    /**
     * To notify when consumption complete.
     */

    private final GraphicsPublisher evaluationStatusPublisher;

    /**
     * Consumer creation lock.
     */

    private final Object consumerCreationLock = new Object();

    /**
     * Consumer of individual messages.
     */

    private StatisticsConsumer consumer;

    /**
     * Consumer for message groups.
     */

    private StatisticsConsumer groupConsumer;

    /**
     * Completion tracker for message groups.
     */

    private final GroupCompletionTracker groupTracker;

    /**
     * A map of group subscribers by group identifier.
     */

    private final Map<String, OneGroupConsumer<Statistics>> groupConsumers;

    /**
     * Is <code>true</code> when the consumers are ready to consume. Until then, cache the statistics.
     */

    private final AtomicBoolean areConsumersReady;

    /**
     * Is <code>true</code> if the evaluation has been closed, otherwise <code>false</code>.
     */

    private final AtomicBoolean isClosed;

    /**
     * Queue of statistics messages and metadata that were received before the consumers were ready.
     */

    private final Queue<StatisticsCache> statisticsCache;

    /**
     * Thread pool to do graphics writing work.
     */

    private final ExecutorService executorService;

    /**
     * Is <code>true</code> if the evaluation has failed, otherwise <code>false</code>.
     */

    private final AtomicBoolean isFailed;

    /**
     * Builds a consumer.
     * 
     * @param evaluationId the evaluation identifier
     * @param consumerId the consumer identifier
     * @param evaluationStatusPublisher the evaluation status publisher
     * @param executorService the executor to do graphics writing work (this instance is not responsible for closing)
     * @throws NullPointerException if any input is null
     */

    EvaluationConsumer( String evaluationId,
                        String consumerId,
                        GraphicsPublisher evaluationStatusPublisher,
                        ExecutorService executorService )
    {
        Objects.requireNonNull( evaluationId );
        Objects.requireNonNull( consumerId );
        Objects.requireNonNull( evaluationStatusPublisher );

        this.evaluationId = evaluationId;
        this.consumerId = consumerId;
        this.evaluationStatusPublisher = evaluationStatusPublisher;
        this.consumed = new AtomicInteger();
        this.expected = new AtomicInteger();
        this.isComplete = new AtomicBoolean();
        this.areConsumersReady = new AtomicBoolean();
        this.isClosed = new AtomicBoolean();
        this.isFailed = new AtomicBoolean();
        this.groupTracker = GroupCompletionTracker.of();
        this.groupConsumers = new ConcurrentHashMap<>();
        this.statisticsCache = new ConcurrentLinkedQueue<>();
        this.executorService = executorService;

        LOGGER.info( "External graphics subscriber {} opened evaluation {}, which is ready to consume messages.",
                     this.consumerId,
                     this.evaluationId );
    }

    /**
     * Closes the evaluation on completion.
     * @throws JMSException if the evaluation failed to close
     */
    void close() throws JMSException
    {
        if ( !this.isClosed() )
        {

            // Flag closed, regardless of what happens next
            this.isClosed.set( true );

            try
            {

                LOGGER.debug( "External graphics subscriber {} is closing evaluation {}.",
                              this.consumerId,
                              this.evaluationId );

                this.completeAllGroups();

                LOGGER.info( "External graphics subscriber {} closed evaluation {}, which contained {} messages (not "
                             + "including any evaluation status messages).",
                             this.consumerId,
                             this.evaluationId,
                             this.consumed.get() );
            }
            catch ( RuntimeException e )
            {
                this.markEvaluationFailed();

                LOGGER.error( "Encountered an error on closing an evaluation consumer.", e );
            }
            finally
            {
                CompletionStatus status = CompletionStatus.CONSUMPTION_COMPLETE_REPORTED_SUCCESS;

                if ( this.failed() )
                {
                    status = CompletionStatus.CONSUMPTION_COMPLETE_REPORTED_FAILURE;
                }

                this.publishCompletionState( status );
            }

            // This instance is not responsible for closing the executor service.
        }
    }

    /**
     * Marks an evaluation as failed unrecoverably after exhausting all attempts to recover the subscriber that 
     * delivers messages to this consumer.
     */

    void markEvaluationFailed()
    {
        this.isFailed.set( true );
    }

    /**
     * Publishes the completion status of the consumer.
     * @param completionStatus the completion status
     * @throws NullPointerException if the input is null
     * @throws JMSException if the status could not be published
     */

    void publishCompletionState( CompletionStatus completionStatus ) throws JMSException
    {
        Objects.requireNonNull( completionStatus );

        // Collect the paths written, if available
        List<String> addThesePaths = new ArrayList<>();

        if ( this.getAreConsumersReady() )
        {
            this.consumer.get()
                         .forEach( next -> addThesePaths.add( next.toString() ) );
            this.groupConsumer.get()
                              .forEach( next -> addThesePaths.add( next.toString() ) );
        }

        // Create the status message to publish
        EvaluationStatus message = EvaluationStatus.newBuilder()
                                                   .setCompletionStatus( completionStatus )
                                                   .setConsumerId( this.consumerId )
                                                   .addAllResourcesCreated( addThesePaths )
                                                   .build();

        // Create the metadata
        String messageId = "ID:" + this.consumerId + "-complete";

        ByteBuffer buffer = ByteBuffer.wrap( message.toByteArray() );

        this.evaluationStatusPublisher.publish( buffer, messageId, this.evaluationId, this.consumerId );

        if ( completionStatus == CompletionStatus.CONSUMPTION_COMPLETE_REPORTED_FAILURE )
        {
            LOGGER.warn( "External graphics subscriber {} has marked evaluation {} as failed unrecoverably.",
                         this.consumerId,
                         this.evaluationId );
        }
    }

    /**
     * Accepts a statistics messages for consumption.
     * @param statistics the statistics
     * @param groupId a message group identifier, which only applies to grouped messages
     * @param messageId the message identifier to help with logging
     */

    void acceptStatisticsMessage( Statistics statistics, String groupId, String messageId )
    {
        Objects.requireNonNull( statistics );
        Objects.requireNonNull( messageId );

        // Consumers ready to consume? 
        // Yes.
        if ( this.getAreConsumersReady() )
        {
            // Accept the incremental types
            this.execute( () -> this.getConsumer()
                                    .accept( List.of( statistics ) ) );

            // Accept the grouped types
            if ( Objects.nonNull( groupId ) )
            {
                this.getGroupConsumer( groupId )
                    .accept( messageId, statistics );
                this.checkAndCompleteGroup( groupId );
            }

            this.consumed.incrementAndGet();

            LOGGER.debug( "External subscriber {} received and consumed a statistics message with identifier {} "
                          + "for evaluation {}.",
                          this.consumerId,
                          messageId,
                          this.evaluationId );
        }
        // No. Cache until the consumers are ready.
        else
        {
            this.statisticsCache.add( new StatisticsCache( statistics, groupId, messageId ) );
        }
    }

    /**
     * Accepts an evaluation description message for consumption.
     * @param evaluationDescription the evaluation description message
     * @param suggestedPath a suggested path string for writing, which is optional
     * @param messageId the message identifier to help with logging
     * @throws IllegalStateException if an evaluation description has already been received
     */

    void acceptEvaluationMessage( Evaluation evaluationDescription, String suggestedPath, String messageId )
    {
        if ( this.getAreConsumersReady() )
        {
            throw new IllegalStateException( "While processing evaluation "
                                             + this.evaluationId
                                             + " in subscriber "
                                             + this.consumerId
                                             + ", encountered two instances of an evaluation description message, "
                                             + "which is not allowed." );
        }

        Objects.requireNonNull( evaluationDescription );

        this.createConsumers( evaluationDescription );

        LOGGER.debug( "External subscriber {} received and consumed an evaluation description message with "
                      + "identifier {} for evaluation {}.",
                      this.consumerId,
                      messageId,
                      this.evaluationId );

        this.consumed.incrementAndGet();
    }

    /**
     * Accepts an evaluation status message for consumption.
     * @param status the evaluation status message
     * @param groupId a message group identifier, which only applies to grouped messages
     * @param messageId the message identifier to help with logging
     */

    void acceptStatusMessage( EvaluationStatus status, String groupId, String messageId )
    {
        Objects.requireNonNull( status );

        LOGGER.debug( "External subscriber {} received and consumed an evaluation status message with identifier {} "
                      + "for evaluation {}.",
                      this.consumerId,
                      messageId,
                      this.evaluationId );

        switch ( status.getCompletionStatus() )
        {
            case GROUP_PUBLICATION_COMPLETE_REPORTED_SUCCESS:
                this.setExpectedMessageCountForGroups( status, groupId );
                break;
            case PUBLICATION_COMPLETE_REPORTED_SUCCESS:
                this.setExpectedMessageCount( status );
                break;
            default:
                break;
        }
    }

    /** 
     * @return true if consumption is complete, otherwise false.
     */

    boolean isComplete()
    {
        // Already registered complete
        if ( this.isComplete.get() )
        {
            return true;
        }

        String append = "of an expected message count that is not yet known";
        if ( this.expected.get() > 0 )
        {
            append = "of an expected " + this.expected.get() + " messages";
        }

        LOGGER.debug( "For evaluation {}, external graphics subscriber {} has consumed {} messages {}.",
                      this.evaluationId,
                      this.consumerId,
                      this.consumed.get(),
                      append );

        this.isComplete.set( this.expected.get() > 0 && this.consumed.get() == this.expected.get() );

        return this.isComplete.get();
    }

    /**
     * @return true if the evaluation has been closed, otherwise false
     */
    boolean isClosed()
    {
        return this.isClosed.get();
    }

    /**
     * @return true if the evaluation failed, otherwise false
     */
    boolean failed()
    {
        return this.isFailed.get();
    }

    /**
     * @return the executor to do graphics writing work.
     */

    private ExecutorService getExecutorService()
    {
        return this.executorService;
    }

    /**
     * Executes a graphics writing task
     * @param task the task to execute
     * @throws GraphicsWriteException if the task fails exceptionally
     */

    private void execute( Runnable task )
    {
        Future<?> future = this.getExecutorService().submit( task );

        // Get and propagate any exception
        try
        {
            future.get();
        }
        catch ( ExecutionException e )
        {
            throw new GraphicsWriteException( "Graphics subscriber " + this.consumerId
                                              + " failed to complete a graphics writing task for evaluation "
                                              + this.evaluationId
                                              + ".",
                                              e );
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();

            throw new GraphicsWriteException( "Graphics subscriber " + this.consumerId
                                              + " failed to complete a graphics writing task for evaluation "
                                              + this.evaluationId
                                              + ".",
                                              e );
        }
    }

    /**
     * Sets the expected message count.
     * @param status the evaluation status message with the expected message count
     */

    private void setExpectedMessageCount( EvaluationStatus status )
    {
        Objects.requireNonNull( status );

        this.expected.addAndGet( status.getMessageCount() );

        LOGGER.debug( "External graphics subscriber {} received notification of publication complete for evaluation "
                      + "{}. The message indicated an expected message count of {}.",
                      this.consumerId,
                      this.evaluationId,
                      this.expected.get() );
    }

    /**
     * Sets the expected message count for message groups.
     * 
     * @param status the evaluation status message
     * @param groupId the message group identifier
     */

    private void setExpectedMessageCountForGroups( EvaluationStatus status,
                                                   String groupId )
    {
        // Set the expected number of messages per group
        this.getGroupTracker()
            .registerGroupComplete( status, groupId );
        boolean completed = this.checkAndCompleteGroup( groupId );

        if ( completed && LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "External graphics subscriber {} received notification of publication complete for group {} "
                          + "of evaluation {}. The message indicated an expected message count of {}.",
                          this.consumerId,
                          groupId,
                          this.evaluationId,
                          status.getMessageCount() );
        }
        else if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "External graphics subscriber {} received notification of publication complete for group {} "
                          + "of evaluation {}. The expected number of messages within the group is {} but some of "
                          + "these messages are outstanding. Grouped consumption will happen when this subscriber is "
                          + "closed.",
                          this.consumerId,
                          groupId,
                          this.evaluationId,
                          status.getMessageCount() );
        }
    }

    /**
     * Completes all message groups prior to closing the consumer.
     * @throws IllegalStateException if the number of messages within the group does not match the expected number
     */

    private void completeAllGroups()
    {
        // Check for group subscriptions that have not completed and complete them, unless this consumer is
        // already in a failure state
        if ( !this.failed() )
        {
            for ( Map.Entry<String, OneGroupConsumer<Statistics>> next : this.groupConsumers.entrySet() )
            {
                String groupId = next.getKey();
                OneGroupConsumer<Statistics> consumerToClose = next.getValue();

                Integer expectedCount = this.getGroupTracker()
                                            .getExpectedMessagesPerGroup( groupId );

                if ( Objects.nonNull( expectedCount ) && !consumerToClose.hasBeenUsed() )
                {
                    if ( consumerToClose.size() != expectedCount )
                    {
                        throw new IllegalStateException( "While attempting to gracefully close subscriber "
                                                         + this
                                                         + " , encountered an error. A consumer of grouped messages "
                                                         + "attached to this subscription expected to receive "
                                                         + expectedCount
                                                         + " messages but actually received "
                                                         + consumerToClose.size()
                                                         + " messages on closing. A subscriber should not be closed "
                                                         + "until consumption is complete." );
                    }

                    // Submit acceptance task
                    this.execute( consumerToClose::acceptGroup );
                }

                LOGGER.trace( "On closing subscriber {}, discovered a consumer associated with group {} whose "
                              + "consumption was ready to complete, but had not yet completed. This were completed.",
                              this,
                              groupId );

            }
        }
    }

    /**
     * @return the group completion tracker for this evaluation.
     */
    private GroupCompletionTracker getGroupTracker()
    {
        return this.groupTracker;
    }

    /**
     * @return true when the consumers are ready to consume, false otherwise.
     */
    private boolean getAreConsumersReady()
    {
        return this.areConsumersReady.get();
    }

    /**
     * @return the incremental consumer.
     */
    private Consumer<Collection<Statistics>> getConsumer()
    {
        return this.consumer;
    }

    /**
     * @param groupId the group identifier
     * @return the group consumer.
     */
    private OneGroupConsumer<Statistics> getGroupConsumer( String groupId )
    {
        Objects.requireNonNull( groupId );

        OneGroupConsumer<Statistics> newGroupConsumer = OneGroupConsumer.of( this.groupConsumer, groupId );
        OneGroupConsumer<Statistics> existingGroupConsumer = this.groupConsumers.putIfAbsent( groupId,
                                                                                              newGroupConsumer );

        if ( Objects.isNull( existingGroupConsumer ) )
        {
            return newGroupConsumer;
        }

        return existingGroupConsumer;
    }

    /**
     * Checks for a complete group and finalizes it.
     * 
     * @param group the group to complete
     * @param consumer the message consumer whose resources should be closed
     * @return true if the group was completed, otherwise false
     */

    private boolean checkAndCompleteGroup( String groupId )
    {
        boolean completed = false;

        if ( Objects.nonNull( this.groupConsumers ) && this.groupConsumers.containsKey( groupId ) )
        {
            OneGroupConsumer<Statistics> check = this.groupConsumers.get( groupId );

            if ( Objects.nonNull( check ) )
            {
                Integer expectedGroupCount = this.getGroupTracker()
                                                 .getExpectedMessagesPerGroup( check.getGroupId() );

                if ( Objects.nonNull( expectedGroupCount ) && expectedGroupCount == check.size()
                     && !check.hasBeenUsed() )
                {
                    this.execute( check::acceptGroup );
                    completed = true;
                }
            }
        }

        return completed;
    }

    /**
     * Attempts to create the consumers.
     * 
     * @param evaluationDescription a description of the evaluation
     */

    private void createConsumers( Evaluation evaluationDescription )
    {
        synchronized ( this.getConsumerCreationLock() )
        {
            if ( Objects.isNull( this.consumer ) )
            {
                LOGGER.debug( "Creating consumers for evaluation {}, which are attached to subscriber {}.",
                              this.evaluationId,
                              this.consumerId );

                // Writable path
                Path pathToWrite = this.getPathToWrite( this.evaluationId );

                // Incremental consumer
                BiPredicate<StatisticType, DestinationType> incrementalTypes =
                        ( type, format ) -> type == StatisticType.BOXPLOT_PER_PAIR
                                            && type != StatisticType.DURATION_SCORE;

                this.consumer = StatisticsConsumer.of( evaluationDescription, incrementalTypes, pathToWrite );

                // Grouped consumer
                BiPredicate<StatisticType, DestinationType> nonIncrementalTypes = incrementalTypes.negate();

                this.groupConsumer = StatisticsConsumer.of( evaluationDescription,
                                                            nonIncrementalTypes,
                                                            pathToWrite );

                LOGGER.debug( "Finished creating consumers for evaluation {}, which are attached to subscriber {}.",
                              this.evaluationId,
                              this.consumerId );

                // Flag that the consumers are ready
                this.areConsumersReady.set( true );
            }

            // Consume any messages that arrived early
            this.consumeCachedStatisticsMessages();
        }
    }

    /**
     * Consumes any statistics messages that arrived before the consumers were ready.
     */

    private void consumeCachedStatisticsMessages()
    {
        // Consume any cached messages that arrived before the consumers were ready and then clear the cache
        if ( !this.statisticsCache.isEmpty() )
        {
            LOGGER.debug( "While consuming evaluation {} in subscriber {}, discovered {} statistics messages that "
                          + "arrived before the consumers were created. These messages were cached and have now been "
                          + "consumed.",
                          this.evaluationId,
                          this.consumerId,
                          this.statisticsCache.size() );

            // Iterate the cache and consume
            for ( StatisticsCache next : this.statisticsCache )
            {
                this.acceptStatisticsMessage( next.getStatistics(), next.getGroupId(), next.getMessageId() );
            }

            // Clear the cache
            this.statisticsCache.clear();
        }
    }

    /**
     * @return the consumer creation lock
     */
    private Object getConsumerCreationLock()
    {
        return this.consumerCreationLock;
    }

    /**
     * Returns a path to write, creating a temporary directory for the outputs with the correct permissions, as needed. 
     *
     * @param evaluationId the evaluation identifier
     * @return the path to the temporary output directory
     * @throws GraphicsWriteException if the temporary directory cannot be created
     * @throws NullPointerException if the evaluationId is null
     */

    private Path getPathToWrite( String evaluationId )
    {
        Objects.requireNonNull( evaluationId );
        
        // Where outputs files will be written
        Path outputDirectory = null;
        String tempDir = System.getProperty( "java.io.tmpdir" );
        
        try
        {
            Path namedPath = Paths.get( tempDir, "wres_evaluation_output_" + evaluationId );
            
            // POSIX-compliant    
            if ( FileSystems.getDefault().supportedFileAttributeViews().contains( "posix" ) )
            {          
                Set<PosixFilePermission> permissions = EnumSet.of( PosixFilePermission.OWNER_READ,
                                                                   PosixFilePermission.OWNER_WRITE,
                                                                   PosixFilePermission.OWNER_EXECUTE,
                                                                   PosixFilePermission.GROUP_READ,
                                                                   PosixFilePermission.GROUP_WRITE,
                                                                   PosixFilePermission.GROUP_EXECUTE );

                FileAttribute<Set<PosixFilePermission>> fileAttribute =
                        PosixFilePermissions.asFileAttribute( permissions );

                // Create if not exists
                outputDirectory = Files.createDirectories( namedPath, fileAttribute );
            }
            // Not POSIX-compliant
            else
            {
                outputDirectory = Files.createDirectories( namedPath );
            }
        }
        catch ( IOException e )
        {
            throw new GraphicsWriteException( "Encountered an error in subscriber " + this.consumerId
                                              + " while attempting to create a temporary "
                                              + "directory for the graphics from evaluation "
                                              + this.evaluationId
                                              + ".",
                                              e );
        }

        return outputDirectory;
    }

    /**
     * Small value object for caching statistics that arrived before the consumers were ready.
     */

    private static class StatisticsCache
    {
        /**The statistics.*/
        private final Statistics statistics;

        /**The group identifier.*/
        private final String groupId;

        /**The message identifier.*/
        private final String messageId;

        private StatisticsCache( Statistics statistics, String groupId, String messageId )
        {
            this.statistics = statistics;
            this.groupId = groupId;
            this.messageId = messageId;
        }

        /**
         * @return the statistics
         */
        private Statistics getStatistics()
        {
            return statistics;
        }

        /**
         * @return the group identifier
         */
        private String getGroupId()
        {
            return groupId;
        }

        /**
         * @return the message identifier
         */
        private String getMessageId()
        {
            return messageId;
        }
    }

}