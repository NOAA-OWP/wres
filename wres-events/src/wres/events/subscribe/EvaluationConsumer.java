package wres.events.subscribe;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
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
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.jms.JMSException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.events.publish.MessagePublisher;
import wres.events.publish.MessagePublisher.MessageProperty;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Consumer;
import wres.statistics.generated.EvaluationStatus;
import wres.statistics.generated.Statistics;
import wres.statistics.generated.EvaluationStatus.CompletionStatus;
import wres.statistics.generated.EvaluationStatus.EvaluationStatusEvent;
import wres.statistics.generated.EvaluationStatus.EvaluationStatusEvent.StatusMessageType;

/**
 * <p>Consumer of messages for one evaluation. Receives messages and forwards them to underlying consumers, which 
 * serialize outputs. The serialization may happen per message or per message group. Where consumption happens per 
 * message group, the {@link EvaluationConsumer} manages the sematics associated with that, forwarding the messages to a 
 * caching consumer until the group has completed and then asking the consumer, finally, to serialize the completed 
 * group. 
 * 
 * <p>Also notifies all listening clients of various stages within the lifecycle of an evaluation or exposes methods 
 * that allow a subscriber to drive that notification. In particular, on closure, notifies all listening clients whether 
 * the evaluation succeeded or failed. Also notifies listening clients when the consumption of a message group has 
 * completed. This notification may be used by a client to trigger/release producer flow control by message group (i.e., 
 * allowing for the publication of another group of messages).
 * 
 * @author james.brown@hydrosolved.com
 */

class EvaluationConsumer
{
    private static final String FAILED_TO_COMPLETE_A_CONSUMPTION_TASK_FOR_EVALUATION =
            " failed to complete a consumption task for evaluation ";

    private static final String SUBSCRIBER = "Subscriber ";

    private static final Logger LOGGER = LoggerFactory.getLogger( EvaluationConsumer.class );

    /**
     * Evaluation identifier.
     */

    private final String evaluationId;

    /**
     * Description of this consumer.
     */

    private final Consumer consumerDescription;

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

    private final MessagePublisher evaluationStatusPublisher;

    /**
     * Consumer creation lock.
     */

    private final Object consumerCreationLock = new Object();

    /**
     * Consumer of individual messages.
     */

    private Function<Statistics, Set<Path>> consumer;

    /**
     * Consumer for message groups.
     */

    private Function<Collection<Statistics>, Set<Path>> groupConsumer;

    /**
     * A map of group consumers by group identifier.
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
     * Thread pool to do writing work.
     */

    private final ExecutorService executorService;

    /**
     * Is <code>true</code> if the evaluation has failed, otherwise <code>false</code>.
     */

    private final AtomicBoolean isFailed;

    /**
     * Is <code>true</code> if the evaluation failure has been notified, otherwise <code>false</code>.
     */

    private final AtomicBoolean isFailureNotified;

    /**
     * A set of paths written by all consumers.
     */

    private final Set<Path> pathsWritten;

    /**
     * The factory that supplies consumers for evaluations.
     */

    private final ConsumerFactory consumerFactory;

    /**
     * Builds a consumer.
     * 
     * @param evaluationId the evaluation identifier
     * @param consumerDescription a description of the consumer
     * @param consumerFactory the consumer factory
     * @param evaluationStatusPublisher the evaluation status publisher
     * @param executorService the executor to do writing work (this instance is not responsible for closing)
     * @throws NullPointerException if any input is null
     */

    EvaluationConsumer( String evaluationId,
                        Consumer consumerDescription,
                        ConsumerFactory consumerFactory,
                        MessagePublisher evaluationStatusPublisher,
                        ExecutorService executorService )
    {
        Objects.requireNonNull( evaluationId );
        Objects.requireNonNull( consumerDescription );
        Objects.requireNonNull( evaluationStatusPublisher );

        this.evaluationId = evaluationId;
        this.evaluationStatusPublisher = evaluationStatusPublisher;
        this.consumed = new AtomicInteger();
        this.expected = new AtomicInteger();
        this.isComplete = new AtomicBoolean();
        this.areConsumersReady = new AtomicBoolean();
        this.isClosed = new AtomicBoolean();
        this.isFailed = new AtomicBoolean();
        this.isFailureNotified = new AtomicBoolean();
        this.groupConsumers = new ConcurrentHashMap<>();
        this.statisticsCache = new ConcurrentLinkedQueue<>();
        this.executorService = executorService;
        this.consumerDescription = consumerDescription;
        this.consumerFactory = consumerFactory;
        this.pathsWritten = new HashSet<>();

        LOGGER.info( "Subscriber {} opened evaluation {}, which is ready to consume messages.",
                     this.getConsumerId(),
                     this.getEvaluationId() );
    }

    /**
     * Closes the evaluation on completion.
     * @throws JMSException if the evaluation failed to close
     * @throws UnrecoverableSubscriberException if the consumer fails unrecoverably in a way that should stop the 
     *            subscriber that wraps it
     */
    void close() throws JMSException
    {
        if ( !this.isClosed() )
        {

            // Flag closed, regardless of what happens next
            this.isClosed.set( true );

            LOGGER.debug( "Subscriber {} is closing evaluation {}.",
                          this.getConsumerId(),
                          this.getEvaluationId() );

            LOGGER.info( "Subscriber {} closed evaluation {}, which contained {} messages (not "
                         + "including any evaluation status messages).",
                         this.getConsumerId(),
                         this.getEvaluationId(),
                         this.consumed.get() );

            if ( this.isFailed() )
            {
                if ( !this.isFailureNotified() )
                {
                    this.publishCompletionState( CompletionStatus.CONSUMPTION_COMPLETE_REPORTED_FAILURE,
                                                 null,
                                                 List.of() );
                }
            }
            else
            {
                this.publishCompletionState( CompletionStatus.CONSUMPTION_COMPLETE_REPORTED_SUCCESS,
                                             null,
                                             List.of() );
            }

            // This instance is not responsible for closing the executor service.
        }
    }

    /**
     * Marks an evaluation as failed unrecoverably after exhausting all attempts to recover the subscriber that 
     * delivers messages to this consumer.
     * @param exception an exception to notify
     */

    void markEvaluationFailed( Exception exception )
    {
        // Notify
        try
        {
            // Create the exception events to notify
            List<EvaluationStatusEvent> events = this.getExceptionEvents( exception );

            this.publishCompletionState( CompletionStatus.CONSUMPTION_COMPLETE_REPORTED_FAILURE,
                                         null,
                                         events );
            this.isFailureNotified.set( true );

            LOGGER.warn( "Subscriber {} has marked evaluation {} as failed unrecoverably.",
                         this.getConsumerId(),
                         this.getEvaluationId() );
        }
        catch ( JMSException e )
        {
            String message = "While marking evaluation " + this.getEvaluationId()
                             + " as "
                             + CompletionStatus.CONSUMPTION_COMPLETE_REPORTED_FAILURE
                             + ": unable to publish the notification.";

            LOGGER.error( message, e );
        }
        finally
        {
            this.isFailed.set( true );
            this.isComplete.set( true );
        }
    }

    /**
     * Publishes the completion status of the consumer.
     * @param completionStatus the completion status
     * @param events evaluation status events
     * @throws NullPointerException if the input is null
     * @throws JMSException if the status could not be published
     */

    void publishCompletionState( CompletionStatus completionStatus, String groupId, List<EvaluationStatusEvent> events )
            throws JMSException
    {
        Objects.requireNonNull( completionStatus );
        Objects.requireNonNull( events );

        // Collect the paths written, if available
        List<String> addThesePaths = this.getPathsWritten()
                                         .stream()
                                         .map( Path::toString )
                                         .collect( Collectors.toUnmodifiableList() );

        // Create the status message to publish
        EvaluationStatus.Builder message = EvaluationStatus.newBuilder()
                                                           .setCompletionStatus( completionStatus )
                                                           .setConsumer( this.getConsumerDescription() )
                                                           .addAllStatusEvents( events )
                                                           .addAllResourcesCreated( addThesePaths );

        if ( Objects.nonNull( groupId ) )
        {
            message.setGroupId( groupId );
        }

        // Create the metadata
        String messageId = "ID:" + this.getConsumerId() + "-complete";

        ByteBuffer buffer = ByteBuffer.wrap( message.build()
                                                    .toByteArray() );

        Map<MessageProperty, String> properties = new EnumMap<>( MessageProperty.class );
        properties.put( MessageProperty.JMS_MESSAGE_ID, messageId );
        properties.put( MessageProperty.JMS_CORRELATION_ID, this.getEvaluationId() );
        properties.put( MessageProperty.CONSUMER_ID, this.getConsumerId() );

        this.evaluationStatusPublisher.publish( buffer, Collections.unmodifiableMap( properties ) );
    }

    /**
     * Accepts a statistics messages for consumption.
     * @param statistics the statistics
     * @param groupId a message group identifier, which only applies to grouped messages
     * @param messageId the message identifier to help with logging
     * @throws JMSException if the group completion could not be notified
     * @throws UnrecoverableSubscriberException if the consumer fails unrecoverably in a way that should stop the 
     *            subscriber that wraps it
     */

    void acceptStatisticsMessage( Statistics statistics,
                                  String groupId,
                                  String messageId )
            throws JMSException
    {
        Objects.requireNonNull( statistics );
        Objects.requireNonNull( messageId );

        // Consumers ready to consume? 
        // Yes.
        if ( this.getAreConsumersReady() )
        {
            this.acceptStatisticsMessageInner( statistics, groupId, messageId );
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
     * @param messageId the message identifier to help with logging
     * @param jobId an optional job identifier
     * @throws JMSException if a consumer could not be created when required
     * @throws UnrecoverableSubscriberException if the consumer fails unrecoverably in a way that should stop the 
     *            subscriber that wraps it
     * @throws IllegalStateException if an evaluation description has already been received
     */

    void acceptEvaluationMessage( Evaluation evaluationDescription, String messageId, String jobId )
            throws JMSException
    {
        if ( this.getAreConsumersReady() )
        {
            throw new IllegalStateException( "While processing evaluation "
                                             + this.getEvaluationId()
                                             + " in subscriber "
                                             + this.getConsumerId()
                                             + ", encountered two instances of an evaluation description message, "
                                             + "which is not allowed." );
        }

        Objects.requireNonNull( evaluationDescription );

        this.createConsumers( evaluationDescription, this.getConsumerFactory(), jobId );

        LOGGER.debug( "Subscriber {} received and consumed an evaluation description message with "
                      + "identifier {} for evaluation {}.",
                      this.getConsumerId(),
                      messageId,
                      this.getEvaluationId() );

        this.consumed.incrementAndGet();
    }

    /**
     * Accepts an evaluation status message for consumption.
     * @param status the evaluation status message
     * @param groupId a message group identifier, which only applies to grouped messages
     * @param messageId the message identifier to help with logging
     * @throws JMSException if a group completion could not be notified
     * @throws UnrecoverableSubscriberException if the consumer fails unrecoverably in a way that should stop the 
     *            subscriber that wraps it
     */

    void acceptStatusMessage( EvaluationStatus status, String groupId, String messageId ) throws JMSException
    {
        Objects.requireNonNull( status );

        LOGGER.debug( "Subscriber {} received and consumed an evaluation status message with identifier {} "
                      + "for evaluation {}.",
                      this.getConsumerId(),
                      messageId,
                      this.getEvaluationId() );

        switch ( status.getCompletionStatus() )
        {
            case GROUP_PUBLICATION_COMPLETE:
                this.setExpectedMessageCountForGroups( status );
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

        LOGGER.debug( "For evaluation {}, external subscriber {} has consumed {} messages {}.",
                      this.getEvaluationId(),
                      this.getConsumerId(),
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
    boolean isFailed()
    {
        return this.isFailed.get();
    }

    /**
     * @return true if the evaluation failure has been notified, otherwise false
     */
    private boolean isFailureNotified()
    {
        return this.isFailureNotified.get();
    }

    /**
     * @return the executor to do writing work.
     */

    private ExecutorService getExecutorService()
    {
        return this.executorService;
    }

    /**
     * Executes a writing task
     * @param task the task to execute
     * @throws ConsumerException if the task fails exceptionally, but potentially in a recoverable way
     * @throws UnrecoverableSubscriberException if the consumer fails unrecoverably in a way that should stop the 
     *            subscriber that wraps it
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
            // Most unchecked exceptions are worth a recovery attempt 
            if ( e.getCause() instanceof RuntimeException )
            {
                throw new ConsumerException( SUBSCRIBER + this.getConsumerId()
                                             + FAILED_TO_COMPLETE_A_CONSUMPTION_TASK_FOR_EVALUATION
                                             + this.getEvaluationId()
                                             + ".",
                                             e );
            }

            throw new UnrecoverableSubscriberException( SUBSCRIBER + this.getConsumerId()
                                                        + FAILED_TO_COMPLETE_A_CONSUMPTION_TASK_FOR_EVALUATION
                                                        + this.getEvaluationId()
                                                        + ".",
                                                        e );
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();

            throw new ConsumerException( SUBSCRIBER + this.getConsumerId()
                                         + FAILED_TO_COMPLETE_A_CONSUMPTION_TASK_FOR_EVALUATION
                                         + this.getEvaluationId()
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

        LOGGER.debug( "Subscriber {} received notification of publication complete for evaluation "
                      + "{}. The message indicated an expected message count of {}.",
                      this.getConsumerId(),
                      this.getEvaluationId(),
                      this.expected.get() );
    }

    /**
     * Creates exception events to notify from an exception.
     * 
     * @param exception the exception
     * @return the exception events
     */

    private List<EvaluationStatusEvent> getExceptionEvents( Exception exception )
    {
        // Nothing to report
        if ( Objects.isNull( exception ) )
        {
            return List.of();
        }

        List<EvaluationStatusEvent> events = new ArrayList<>();

        EvaluationStatusEvent event = EvaluationStatusEvent.newBuilder()
                                                           .setEventType( StatusMessageType.ERROR )
                                                           .setEventMessage( exception.getMessage() )
                                                           .build();

        events.add( event );

        // Add up to five causes, where available
        Throwable cause = exception.getCause();

        for ( int i = 0; i < 5; i++ )
        {
            if ( Objects.nonNull( cause ) )
            {
                EvaluationStatusEvent eventInner = EvaluationStatusEvent.newBuilder()
                                                                        .setEventType( StatusMessageType.ERROR )
                                                                        .setEventMessage( cause.getMessage() )
                                                                        .build();

                events.add( eventInner );
            }
            else
            {
                break;
            }

            cause = cause.getCause();
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Sets the expected message count for message groups.
     * 
     * @param status the evaluation status message
     * @throws JMSException if the group completion could not be notified
     * @throws UnrecoverableSubscriberException if the consumer fails unrecoverably in a way that should stop the 
     *            subscriber that wraps it
     * @throws JMSException if the completion state could not be published
     */

    private void setExpectedMessageCountForGroups( EvaluationStatus status ) throws JMSException
    {
        // Set the expected number of messages per group
        String groupId = status.getGroupId();

        Objects.requireNonNull( groupId );

        if ( status.getCompletionStatus() != CompletionStatus.GROUP_PUBLICATION_COMPLETE )
        {
            throw new IllegalArgumentException( "While registered the expected message count of group " + groupId
                                                + ", received an unexpected completion "
                                                + "status  "
                                                + status.getCompletionStatus()
                                                + ". Expected "
                                                + CompletionStatus.GROUP_PUBLICATION_COMPLETE );
        }

        if ( status.getMessageCount() == 0 )
        {
            throw new IllegalArgumentException( "The completion status message for group " + groupId
                                                + " is missing an expected count of messages." );
        }

        // Set the expected count
        OneGroupConsumer<Statistics> groupCon = this.getGroupConsumer( groupId );
        groupCon.setExpectedMessageCount( status.getMessageCount() );

        this.checkAndCompleteGroup( groupCon );
    }

    /**
     * @return the evaluation identifier.
     */

    private String getEvaluationId()
    {
        return this.evaluationId;
    }

    /**
     * @return the consumer factory.
     */

    private ConsumerFactory getConsumerFactory()
    {
        return this.consumerFactory;
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
    private Function<Statistics, Set<Path>> getConsumer()
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

        OneGroupConsumer<Statistics> newGroupConsumer = OneGroupConsumer.of( this.groupConsumer::apply, groupId );
        OneGroupConsumer<Statistics> existingGroupConsumer = this.groupConsumers.putIfAbsent( groupId,
                                                                                              newGroupConsumer );

        if ( Objects.isNull( existingGroupConsumer ) )
        {
            return newGroupConsumer;
        }

        return existingGroupConsumer;
    }

    /**
     * Attempts to create the consumers.
     * 
     * @param evaluationDescription a description of the evaluation
     * @param consumerFactory the consumer factory
     * @param jobId an optional job identifier
     * @throws JMSException if a group completion could not be notified
     * @throws UnrecoverableSubscriberException if the consumer fails unrecoverably in a way that should stop the 
     *            subscriber that wraps it
     */

    private void createConsumers( Evaluation evaluationDescription,
                                  ConsumerFactory consumerFactory,
                                  String jobId )
            throws JMSException
    {
        synchronized ( this.getConsumerCreationLock() )
        {
            if ( !this.getAreConsumersReady() )
            {
                LOGGER.debug( "Creating consumers for evaluation {}, which are attached to subscriber {}.",
                              this.getEvaluationId(),
                              this.getConsumerId() );

                // Create a path to write
                Path path = ConsumerFactory.getPathToWrite( this.getEvaluationId(), this.getConsumerId(), jobId );

                this.consumer = consumerFactory.getConsumer( evaluationDescription, path );
                this.groupConsumer = consumerFactory.getGroupedConsumer( evaluationDescription, path );

                LOGGER.debug( "Finished creating consumers for evaluation {}, which are attached to subscriber {}.",
                              this.getEvaluationId(),
                              this.getConsumerId() );

                // Consume any messages that arrived early
                this.consumeCachedStatisticsMessages();

                // Flag that the consumers are ready
                this.areConsumersReady.set( true );
            }
        }
    }

    /**
     * Consumes any statistics messages that arrived before the consumers were ready.
     * @throws JMSException if a group completion could not be notified 
     * @throws UnrecoverableSubscriberException if the consumer fails unrecoverably in a way that should stop the 
     *            subscriber that wraps it
     */

    private void consumeCachedStatisticsMessages() throws JMSException
    {
        // Consume any cached messages that arrived before the consumers were ready and then clear the cache
        if ( !this.statisticsCache.isEmpty() )
        {
            LOGGER.debug( "While consuming evaluation {} in subscriber {}, discovered {} statistics messages that "
                          + "arrived before the consumers were created. These messages were cached and have now been "
                          + "consumed.",
                          this.getEvaluationId(),
                          this.getConsumerId(),
                          this.statisticsCache.size() );

            // Iterate the cache and consume
            for ( StatisticsCache next : this.statisticsCache )
            {
                // Internal call, which does not have logic for caching statistics because the present context is to
                // process cached statistics.
                this.acceptStatisticsMessageInner( next.getStatistics(), next.getGroupId(), next.getMessageId() );
            }

            // Clear the cache
            this.statisticsCache.clear();
        }
    }

    /**
     * Inner method to accepts statistics. This method does not contain logic for caching statistics when consumers are 
     * not ready.
     * @param statistics the statistics
     * @param groupId a message group identifier, which only applies to grouped messages
     * @param messageId the message identifier to help with logging
     * @throws JMSException if the group completion could not be notified
     * @throws UnrecoverableSubscriberException if the consumer fails unrecoverably in a way that should stop the 
     *            subscriber that wraps it
     */

    private void acceptStatisticsMessageInner( Statistics statistics, String groupId, String messageId )
            throws JMSException
    {
        // Accept the incremental types
        this.execute( () -> this.addPathsWritten( this.getConsumer()
                                                      .apply( statistics ) ) );

        // Accept the grouped types
        if ( Objects.nonNull( groupId ) )
        {
            OneGroupConsumer<Statistics> groupCon = this.getGroupConsumer( groupId );
            groupCon.accept( messageId, statistics );

            this.checkAndCompleteGroup( groupCon );
        }

        this.consumed.incrementAndGet();

        LOGGER.debug( "Subscriber {} received and consumed a statistics message with identifier {} "
                      + "for evaluation {}.",
                      this.getConsumerId(),
                      messageId,
                      this.getEvaluationId() );
    }

    /**
     * Checks the group consumer for completion and, if complete, publishes a status message 
     * {@link CompletionStatus#GROUP_CONSUMPTION_COMPLETE} and updates the paths written.
     * 
     * @param groupCon the group consumer
     * @throws JMSException if the completion state could not be published
     * @throws NullPointerException if the input is null
     */

    private void checkAndCompleteGroup( OneGroupConsumer<Statistics> groupCon ) throws JMSException
    {
        if ( groupCon.isComplete() )
        {
            // Add any paths created if the group has completed
            Set<Path> paths = groupCon.get();
            this.addPathsWritten( paths );

            // Notify completion
            this.publishCompletionState( CompletionStatus.GROUP_CONSUMPTION_COMPLETE,
                                         groupCon.getGroupId(),
                                         List.of() );

            LOGGER.debug( "Subscriber {} registered consumption complete for group {} of evaluation {}.",
                          this.consumerDescription.getConsumerId(),
                          groupCon.getGroupId(),
                          this.getEvaluationId() );
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
     * Adds paths written to the cache of all paths written.
     * 
     * @param paths the paths to append
     */

    private void addPathsWritten( Set<Path> paths )
    {
        this.pathsWritten.addAll( paths );
    }

    /**
     * @return an immutable view of the paths written so far.
     */

    private Set<Path> getPathsWritten()
    {
        return Collections.unmodifiableSet( this.pathsWritten );
    }

    /**
     * @return the consumer description
     */

    private Consumer getConsumerDescription()
    {
        return this.consumerDescription;
    }

    /**
     * @return the consumer identifier
     */

    private String getConsumerId()
    {
        return this.consumerDescription.getConsumerId();
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