package wres.events;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.naming.NamingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;

import net.jcip.annotations.Immutable;
import wres.eventsbroker.BrokerConnectionFactory;
import wres.statistics.generated.EvaluationStatus;
import wres.statistics.generated.EvaluationStatus.CompletionStatus;
import wres.statistics.generated.EvaluationStatus.EvaluationStatusEvent;
import wres.statistics.generated.EvaluationStatus.EvaluationStatusEvent.StatusMessageType;
import wres.statistics.generated.Statistics;

/**
 * <p>Manages the publication and consumption of messages associated with one evaluation, defined as a single 
 * computational instance of one project declaration. Manages the publication of, and subscription to, the following 
 * types of messages that map to evaluation events:
 * 
 * <ol>
 * <li>Evaluation messages contained in {@link wres.statistics.generated.Evaluation};</li>
 * <li>Evaluation status messages contained in {@link wres.statistics.generated.EvaluationStatus}; and</li>
 * <li>Statistics messages contained in {@link wres.statistics.generated.Statistics}.</li>
 * </ol>
 * 
 * <p>An evaluation is assigned a unique identifier on construction. This identifier is used to correlate messages that
 * belong to the same evaluation.
 * 
 * <p>Currently, this is intended for internal use by java producers and consumers within the core of the wres. In 
 * future, it is envisaged that an "advanced" API will be exposed to external clients that can post evaluations and 
 * register consumers to consume all types of evaluation messages. This advanced API would provide developers of 
 * microservices an alternative route, alongside the RESTful API, to publish and subscribe to evaluations, adding more
 * advanced possibilities, such as asynchronous messaging and streaming. This API will probably leverage a 
 * request-response pattern, such as gRPC (www.grpc.io), in order to register an evaluation and obtain the evaluation 
 * identifier and connection details for brokered (i.e., non request-response) communication.
 * 
 * @author james.brown@hydrosolved.com
 */

@Immutable
public class Evaluation implements Closeable
{

    private static final Logger LOGGER = LoggerFactory.getLogger( Evaluation.class );

    private static final String DISCOVERED_AN_EVALUATION_MESSAGE_WITH_MISSING_INFORMATION =
            " discovered an evaluation message with missing information. ";

    private static final String WHILE_PUBLISHING_TO_EVALUATION = "While publishing to evaluation ";

    private static final String CANNOT_BUILD_AN_EVALUATION_WITHOUT_ONE_OR_MORE_SUBSCRIBERS =
            "Cannot build an evaluation without one or more subscribers for ";

    private static final String MESSAGE_FROM_A_BYTEBUFFER = " message from a bytebuffer.";

    /**
     * Used to generate unique evaluation identifiers.
     */

    private static final RandomString ID_GENERATOR = new RandomString();

    /**
     * Default name for the amq.topic that accepts evaluation messages.
     */

    private static final String EVALUATION_QUEUE = "evaluation";

    /**
     * Default name for the amq.topic that accepts evaluation status messages.
     */

    private static final String EVALUATION_STATUS_QUEUE = "status";

    /**
     * Default name for the amq.topic that accepts statistics messages.
     */

    private static final String STATISTICS_QUEUE = "statistics";

    /**
     * A publisher for {@link wres.statistics.generated.Evaluation} messages.
     */

    private final MessagePublisher evaluationPublisher;

    /**
     * A publisher for {@link EvaluationStatus} messages.
     */

    private final MessagePublisher evaluationStatusPublisher;

    /**
     * A publisher for {@link Statistics} messages.
     */

    private final MessagePublisher statisticsPublisher;

    /**
     * A collection of subscribers for {@link wres.statistics.generated.Evaluation} messages.
     */

    private final MessageSubscriber<wres.statistics.generated.Evaluation> evaluationSubscribers;

    /**
     * A collection of subscribers for {@link wres.statistics.generated.Statistics} messages.
     */

    private final MessageSubscriber<Statistics> statisticsSubscribers;

    /**
     * A collection of subscribers for {@link EvaluationStatus} messages.
     */

    private final MessageSubscriber<EvaluationStatus> evaluationStatusSubscribers;

    /**
     * A unique identifier for the evaluation.
     */

    private final String evaluationId;

    /**
     * Is <code>true</code> if this evaluation has one or more subscribers for message groups. If <code>true</code>,
     * then any attempt to publish a message must be accompanied by a message group identifier.
     */

    private final boolean hasGroupSubscriptions;

    /**
     * The total message count, excluding evaluation status messages. This is one more than the number of statistics
     * messages because an evaluation begins with an evaluation description message.
     */

    private final AtomicInteger messageCount;

    /**
     * The total number of evaluation status messages.
     */

    private final AtomicInteger statusMessageCount;

    /**
     * Monitors the completion state of an evaluation, allowing for a graceful exit.
     */

    private final CompletionTracker completionTracker;

    /**
     * An object that contains a completion status message and a latch that counts to zero when an evaluation has been 
     * notified complete.
     */

    private final CompletionStatusEvent completionEvent;

    /**
     * Returns the unique evaluation identifier.
     * 
     * @return the evaluation identifier
     */

    public String getEvaluationId()
    {
        return this.evaluationId;
    }

    /**
     * Opens an evaluation.
     * 
     * @param evaluation the evaluation message
     * @param broker the broker
     * @param consumers the consumers to subscribe
     * @return an open evaluation
     * @throws NullPointerException if any input is null
     */

    public static Evaluation open( wres.statistics.generated.Evaluation evaluation,
                                   BrokerConnectionFactory broker,
                                   Consumers consumers )
    {
        Objects.requireNonNull( evaluation );
        Objects.requireNonNull( broker );
        Objects.requireNonNull( consumers );

        return new Builder().setBroker( broker )
                            .setEvaluation( evaluation )
                            .setConsumers( consumers )
                            .build();
    }

    /**
     * Publish an {@link wres.statistics.generated.EvaluationStatus} message for the current evaluation.
     * 
     * @param status the status message
     */

    public void publish( EvaluationStatus status )
    {
        this.publish( status, null );
    }

    /**
     * Publish an {@link wres.statistics.generated.Statistics} message for the current evaluation.
     * 
     * @param statistics the statistics message
     */

    public void publish( Statistics statistics )
    {
        this.publish( statistics, null );
    }

    /**
     * Publish an {@link wres.statistics.generated.EvaluationStatus} message for the current evaluation.
     * 
     * @param status the status message
     * @param groupId an optional group identifier to identify grouped status messages (required if group subscribers)
     * @throws NullPointerException if the message is null or the groupId is null when there are group subscriptions
     */

    public void publish( EvaluationStatus status, String groupId )
    {
        this.validateStatusMessage( status, groupId );

        ByteBuffer body = ByteBuffer.wrap( status.toByteArray() );

        this.internalPublish( body, this.evaluationStatusPublisher, Evaluation.EVALUATION_STATUS_QUEUE, groupId );

        this.statusMessageCount.getAndIncrement();
    }

    /**
     * Publish an {@link wres.statistics.generated.Statistics} message for the current evaluation.
     * 
     * @param statistics the statistics message
     * @param groupId an optional group identifier to identify grouped status messages (required if group subscribers)
     * @throws NullPointerException if the message is null or the groupId is null when there are group subscriptions
     */

    public void publish( Statistics statistics, String groupId )
    {
        Objects.requireNonNull( statistics );
        this.validateGroupId( groupId );

        ByteBuffer body = ByteBuffer.wrap( statistics.toByteArray() );

        this.internalPublish( body, this.statisticsPublisher, Evaluation.STATISTICS_QUEUE, groupId );

        this.messageCount.getAndIncrement();

        // Update the status
        String message = "Published a new statistics message for evaluation " + this.getEvaluationId()
                         + " with pool boundaries "
                         + statistics.getPool();

        EvaluationStatus ongoing = EvaluationStatus.newBuilder()
                                                   .setCompletionStatus( CompletionStatus.ONGOING )
                                                   .addStatusEvents( EvaluationStatusEvent.newBuilder()
                                                                                          .setEventType( StatusMessageType.INFO )
                                                                                          .setEventMessage( message ) )
                                                   .build();

        this.publish( ongoing, groupId );
    }

    @Override
    public String toString()
    {
        return "Evaluation with unique identifier: " + evaluationId;
    }

    @Override
    public void close() throws IOException
    {
        LOGGER.debug( "Closing evaluation {} gracefully.", this.getEvaluationId() );

        LOGGER.debug( "Awaiting receipt of completion status message for evaluation {}...", this.getEvaluationId() );

        Instant then = Instant.now();

        try
        {
            this.completionEvent.await();
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();

            throw new EvaluationEventException( "Interrupted while waiting for completion status event for evaluuation "
                                                + this.getEvaluationId() );
        }

        EvaluationStatus completionMessage = this.completionEvent.getStatus();

        LOGGER.debug( "Received notification of completion for evaluation {}. The completion message is {}"
                      + "Awaiting local consumption...",
                      this.getEvaluationId(),
                      completionMessage );

        // Awaiting the completion notifier implicitly asserts that the number of messages published and consumed is
        // equal because the notifier tracks the number of actual consumptions and obtains the expected number of 
        // consumptions from the completion event message. If the actual consumption count is smaller, the evaluation 
        // will wait until a safety timeout that resets on progress. If it is larger, an exception can be expected.
        // See the CompletionTracker for details.
        try
        {
            this.completionTracker.await( completionMessage );
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();

            throw new EvaluationEventException( "Interrupted while waiting for completion of evaluation "
                                                + this.getEvaluationId() );
        }
        catch ( IllegalArgumentException f )
        {
            throw new EvaluationEventException( "Failure while waiting for the completion of evaluation "
                                                + this.getEvaluationId()
                                                + " with completion status: "
                                                + this.completionTracker,
                                                f );
        }

        Instant now = Instant.now();

        LOGGER.debug( "Completed local consumption for evaluation {} and received completition notifier, {} {} "
                      + "elapsed between notification of completion and the end of consumption. Ready to validate and "
                      + "then close this evaluation.",
                      this.getEvaluationId(),
                      this.completionTracker,
                      Duration.between( then, now ) );

        // Exceptions uncovered?
        // Perhaps failing on the delivery of evaluation status messages is too high a bar?
        boolean failed = Objects.nonNull( this.evaluationSubscribers.getFailedOn() )
                         || Objects.nonNull( this.evaluationStatusSubscribers.getFailedOn() )
                         || Objects.nonNull( this.statisticsSubscribers.getFailedOn() );

        if ( failed )
        {
            String separator = System.getProperty( "line.separator" );

            LOGGER.debug( "While closing evaluation {}, discovered one or more undeliverable messages. The first "
                          + "undeliverable evaluation message is:{}{}{}The first undeliverable evaluation status "
                          + "message is:{}{}{}The first undeliverable statistics message is:{}{}",
                          this.getEvaluationId(),
                          separator,
                          this.evaluationSubscribers.getFailedOn(),
                          separator,
                          separator,
                          this.evaluationStatusSubscribers.getFailedOn(),
                          separator,
                          separator,
                          this.statisticsSubscribers.getFailedOn() );

            this.evaluationPublisher.close();
            this.evaluationSubscribers.close();
            this.evaluationStatusPublisher.close();
            this.evaluationStatusSubscribers.close();
            this.statisticsPublisher.close();
            this.statisticsSubscribers.close();

            throw new EvaluationEventException( "While closing evaluation " + this.getEvaluationId()
                                                + ", discovered undeliverable messages after repeated delivery "
                                                + "attempts. These messages have been posted to their appropriate dead "
                                                + "letter queue (DLQ) for further inspection and removal." );
        }

        this.evaluationPublisher.close();
        this.evaluationSubscribers.close();
        this.evaluationStatusPublisher.close();
        this.evaluationStatusSubscribers.close();
        this.statisticsPublisher.close();
        this.statisticsSubscribers.close();

        LOGGER.debug( "Closed evaluation {}.", this.getEvaluationId() );
    }

    /**
     * Returns the expected number of messages, excluding evaluation status messages. This is one more than the number
     * of statistics messages because an evaluation starts with an evaluation description message.
     * 
     * @return the published message count
     */

    public int getPublishedMessageCount()
    {
        return this.messageCount.get();
    }

    /**
     * Returns the expected number of evaluation status messages.
     * 
     * @return the evaluation status message count
     */

    public int getPublishedStatusMessageCount()
    {
        return this.statusMessageCount.get();
    }

    /**
     * Builds an evaluation.
     * 
     * @author james.brown@hydrosolved.com
     */

    public static class Builder
    {
        /**
         * Broker connections.
         */

        private BrokerConnectionFactory broker;

        /**
         * Evaluation message.
         */

        private wres.statistics.generated.Evaluation evaluation;

        /**
         * Consumers of evaluation events.
         */

        private Consumers consumers;

        /**
         * Sets the broker.
         * 
         * @param broker the broker
         * @return this builder
         */

        public Builder setBroker( BrokerConnectionFactory broker )
        {
            this.broker = broker;

            return this;
        }

        /**
         * Sets the evaluation message containing an overview of the evaluation.
         * 
         * @param evaluation the evaluation
         * @return this builder
         */

        public Builder setEvaluation( wres.statistics.generated.Evaluation evaluation )
        {
            this.evaluation = evaluation;

            return this;
        }

        /**
         * Adds a collection of consumers of evaluation events.
         * 
         * @param consumers the consumer subscription
         * @return this builder 
         */

        public Builder setConsumers( Consumers consumers )
        {
            this.consumers = consumers;

            return this;
        }

        /**
         * Builds an evaluation.
         * 
         * @return an evaluation
         */

        public Evaluation build()
        {
            return new Evaluation( this );
        }
    }

    /**
     * Validates a status message for expected content and looks for the presence of a group identifier where required. 
     * 
     * @param groupId a group identifier
     * @throws NullPointerException if the group identifier is null
     */

    private void validateStatusMessage( EvaluationStatus message, String groupId )
    {
        Objects.requireNonNull( message );

        // Group identifier required in some cases
        if ( message.getCompletionStatus() == CompletionStatus.GROUP_COMPLETE_REPORTED_SUCCESS )
        {
            this.validateGroupId( groupId );

            if ( message.getMessageCount() == 0 )
            {
                throw new IllegalArgumentException( WHILE_PUBLISHING_TO_EVALUATION + this.getEvaluationId()
                                                    + DISCOVERED_AN_EVALUATION_MESSAGE_WITH_MISSING_INFORMATION
                                                    + "The expected message count is required when the completion "
                                                    + "status reports "
                                                    + CompletionStatus.GROUP_COMPLETE_REPORTED_SUCCESS );
            }
        }

        if ( message.getCompletionStatus() == CompletionStatus.COMPLETE_REPORTED_SUCCESS )
        {

            if ( message.getMessageCount() == 0 )
            {
                throw new IllegalArgumentException( WHILE_PUBLISHING_TO_EVALUATION + this.getEvaluationId()
                                                    + DISCOVERED_AN_EVALUATION_MESSAGE_WITH_MISSING_INFORMATION
                                                    + "The expected message count is required when the completion "
                                                    + "status reports "
                                                    + CompletionStatus.COMPLETE_REPORTED_SUCCESS );
            }
            if ( message.getStatusMessageCount() == 0 )
            {
                throw new IllegalArgumentException( WHILE_PUBLISHING_TO_EVALUATION + this.getEvaluationId()
                                                    + DISCOVERED_AN_EVALUATION_MESSAGE_WITH_MISSING_INFORMATION
                                                    + "The expected count of evaluation status messages is required "
                                                    + "when the completion status reports "
                                                    + CompletionStatus.COMPLETE_REPORTED_SUCCESS );
            }
        }
    }

    /**
     * Looks for the presence of a group identifier and throws an exception when absent.
     * 
     * @param groupId a group identifier
     * @throws NullPointerException if the group identifier is null
     */

    private void validateGroupId( String groupId )
    {
        if ( this.hasGroupSubscriptions() )
        {
            Objects.requireNonNull( groupId,
                                    "Evaluation " + this.getEvaluationId()
                                             + " has subscriptions to message groups. When publishing to this "
                                             + "evaluation, a group identifier must be supplied." );
        }
    }

    /**
     * Returns <code>true</code> if the evaluation has subscriptions for message groups, otherwise <code>false</code>.
     * 
     * @return true if there are subscriptions for message groups
     */

    private boolean hasGroupSubscriptions()
    {
        return this.hasGroupSubscriptions;
    }

    /**
     * Builds an exception with the specified message.
     * 
     * @param builder the builder
     * @throws EvaluationEventException if the evaluation could not be constructed for any reason
     * @throws NullPointerException if the broker is null
     * @throws IllegalArgumentException if there are zero subscribers for any queue 
     * @throws IllegalStateException if the evaluation message fails to declare the expected number of pools
     */

    private Evaluation( Builder builder )
    {
        // Copy then validate
        BrokerConnectionFactory broker = builder.broker;
        List<Consumer<wres.statistics.generated.Evaluation>> evaluationSubs =
                new ArrayList<>( builder.consumers.getEvaluationConsumers() );
        List<Consumer<EvaluationStatus>> statusSubs =
                new ArrayList<>( builder.consumers.getEvaluationStatusConsumers() );
        List<Consumer<Statistics>> statisticsSubs = new ArrayList<>( builder.consumers.getStatisticsConsumers() );
        List<Consumer<Statistics>> groupedStatisticsSubs =
                new ArrayList<>( builder.consumers.getGroupedStatisticsConsumers() );

        wres.statistics.generated.Evaluation evaluationMessage = builder.evaluation;

        Objects.requireNonNull( broker, "Cannot create an evaluation without a broker connection." );
        Objects.requireNonNull( evaluationMessage, "Cannot create an evaluation without an evaluation message." );

        if ( evaluationSubs.isEmpty() )
        {
            throw new IllegalArgumentException( CANNOT_BUILD_AN_EVALUATION_WITHOUT_ONE_OR_MORE_SUBSCRIBERS
                                                + "evaluation events." );
        }

        if ( statusSubs.isEmpty() )
        {
            throw new IllegalArgumentException( CANNOT_BUILD_AN_EVALUATION_WITHOUT_ONE_OR_MORE_SUBSCRIBERS
                                                + "evaluation status events." );
        }

        if ( statisticsSubs.isEmpty() && groupedStatisticsSubs.isEmpty() )
        {
            throw new IllegalArgumentException( CANNOT_BUILD_AN_EVALUATION_WITHOUT_ONE_OR_MORE_SUBSCRIBERS
                                                + "evaluation statistics events." );
        }

        int expectedStatisticsMessageCount = evaluationMessage.getPoolMessageCount();

        if ( expectedStatisticsMessageCount <= 0 )
        {
            throw new IllegalStateException( "An evaluation message failed to declare the expected number of pools "
                                             + "(of statistics) produced by the evaluation, which is necessary to "
                                             + "track and close the evaluation on successful completion." );
        }

        this.evaluationId = Evaluation.ID_GENERATOR.generate();
        this.hasGroupSubscriptions = !groupedStatisticsSubs.isEmpty();

        // Completion subscriber that tracks the completion status
        this.completionEvent = new CompletionStatusEvent();

        // Register with status subscribers: one more consumer
        statusSubs.add( this.completionEvent );

        this.completionTracker = CompletionTracker.of( evaluationSubs.size(),
                                                       statisticsSubs.size(),
                                                       statusSubs.size(),
                                                       groupedStatisticsSubs.size() );

        // Register publishers and subscribers
        try
        {
            ConnectionFactory factory = broker.get();

            Destination status = broker.getDestination( Evaluation.EVALUATION_STATUS_QUEUE );
            this.evaluationStatusPublisher = MessagePublisher.of( factory, status );
            this.evaluationStatusSubscribers =
                    new MessageSubscriber.Builder<EvaluationStatus>().setConnectionFactory( factory )
                                                                     .setDestination( status )
                                                                     .addSubscribers( statusSubs )
                                                                     .setCompletionTracker( this.completionTracker )
                                                                     .setMapper( this.getStatusMapper() )
                                                                     .setEvaluationId( this.getEvaluationId() )
                                                                     .build();

            Destination evaluation = broker.getDestination( Evaluation.EVALUATION_QUEUE );
            this.evaluationPublisher = MessagePublisher.of( factory, evaluation );
            this.evaluationSubscribers =
                    new MessageSubscriber.Builder<wres.statistics.generated.Evaluation>().setConnectionFactory( factory )
                                                                                         .setDestination( evaluation )
                                                                                         .addSubscribers( evaluationSubs )
                                                                                         .setCompletionTracker( this.completionTracker )
                                                                                         .setMapper( this.getEvaluationMapper() )
                                                                                         .setEvaluationId( this.getEvaluationId() )
                                                                                         .build();

            Destination statistics = broker.getDestination( Evaluation.STATISTICS_QUEUE );
            this.statisticsPublisher = MessagePublisher.of( factory, statistics );

            Function<List<Statistics>, Statistics> aggregator = null;
            if ( this.hasGroupSubscriptions() )
            {
                aggregator = OneGroupConsumer.getStatisticsAggregator();
            }

            this.statisticsSubscribers =
                    new MessageSubscriber.Builder<Statistics>().setConnectionFactory( factory )
                                                               .setDestination( statistics )
                                                               .addSubscribers( statisticsSubs )
                                                               .setCompletionTracker( this.completionTracker )
                                                               .addGroupSubscribers( groupedStatisticsSubs )
                                                               .setGroupAggregator( aggregator )
                                                               .setEvaluationStatusDestination( status )
                                                               .setMapper( this.getStatisticsMapper() )
                                                               .setEvaluationId( this.getEvaluationId() )
                                                               .build();
        }
        catch ( JMSException | NamingException e )
        {
            throw new EvaluationEventException( "Unable to construct an evaluation.", e );
        }

        LOGGER.info( "Created a new evaluation with id {}.", evaluationId );

        this.messageCount = new AtomicInteger();
        this.statusMessageCount = new AtomicInteger();

        // Publish the evaluation and update the evaluation status
        this.internalPublish( evaluationMessage );

        Instant now = Instant.now();
        long seconds = now.getEpochSecond();
        int nanos = now.getNano();
        EvaluationStatus ongoing = EvaluationStatus.newBuilder()
                                                   .setCompletionStatus( CompletionStatus.ONGOING )
                                                   .setEvaluationStartTime( Timestamp.newBuilder()
                                                                                     .setSeconds( seconds )
                                                                                     .setNanos( nanos ) )
                                                   .build();

        this.publish( ongoing );
    }

    /**
     * Maps a message contained in a {@link ByteBuffer} to a {@link EvaluationStatus}.
     * @return a mapper
     */

    private Function<ByteBuffer, EvaluationStatus> getStatusMapper()
    {
        return buffer -> {
            try
            {
                return EvaluationStatus.parseFrom( buffer );
            }
            catch ( InvalidProtocolBufferException e )
            {
                throw new EvaluationEventException( "While processing an evaluation status event for evaluation "
                                                    + this.getEvaluationId()
                                                    + ", failed to create an evaluation status "
                                                    + MESSAGE_FROM_A_BYTEBUFFER,
                                                    e );
            }
        };
    }

    /**
     * Maps a message contained in a {@link ByteBuffer} to a {@link wres.statistics.generated.Evaluation}.
     * @return a mapper
     */

    private Function<ByteBuffer, wres.statistics.generated.Evaluation> getEvaluationMapper()
    {
        return buffer -> {
            try
            {
                return wres.statistics.generated.Evaluation.parseFrom( buffer );
            }
            catch ( InvalidProtocolBufferException e )
            {
                throw new EvaluationEventException( "While processing an evaluation event for evaluation "
                                                    + this.getEvaluationId()
                                                    + ", failed to create an evaluation "
                                                    + MESSAGE_FROM_A_BYTEBUFFER,
                                                    e );
            }
        };
    }

    /**
     * Maps a message contained in a {@link ByteBuffer} to a {@link Statistics}.
     * @return a mapper
     */

    private Function<ByteBuffer, Statistics> getStatisticsMapper()
    {
        return buffer -> {
            try
            {
                return Statistics.parseFrom( buffer );
            }
            catch ( InvalidProtocolBufferException e )
            {
                throw new EvaluationEventException( "While processing a statistics event for evaluation "
                                                    + this.getEvaluationId()
                                                    + ", failed to create a statistics "
                                                    + MESSAGE_FROM_A_BYTEBUFFER,
                                                    e );
            }
        };
    }

    /**
     * Internal publish, do not expose.
     * 
     * @param body the message body
     * @param publisher the publisher
     * @param queue the queue name on the amq.topic
     * @param groupId the optional message group identifier
     */

    private void internalPublish( ByteBuffer body, MessagePublisher publisher, String queue, String groupId )
    {
        // Published below, so increment by 1 here 
        String messageId = "ID:" + this.getEvaluationId() + "-m" + ( publisher.getMessageCount() + 1 );

        try
        {
            publisher.publish( body, messageId, this.getEvaluationId(), groupId );

            LOGGER.info( "Published a message with identifier {} and correlation identifier {} for evaluation {} to "
                         + "amq.topic/{}.",
                         messageId,
                         this.getEvaluationId(),
                         this.getEvaluationId(),
                         queue );
        }
        catch ( JMSException e )
        {
            throw new EvaluationEventException( "Failed to send a message for evaluation "
                                                + this.getEvaluationId()
                                                + " to amq.topic/"
                                                + queue,
                                                e );
        }
    }

    /**
     * Publish an {@link wres.statistics.generated.Evaluation} message for the current evaluation.
     * 
     * @param evaluation the evaluation message
     */

    private void internalPublish( wres.statistics.generated.Evaluation evaluation )
    {
        Objects.requireNonNull( evaluation );

        ByteBuffer body = ByteBuffer.wrap( evaluation.toByteArray() );

        this.internalPublish( body, this.evaluationPublisher, Evaluation.EVALUATION_QUEUE, null );

        this.messageCount.getAndIncrement();
    }

    /**
     * Generate a compact, unique, identifier for an evaluation. Thanks to: 
     * https://neilmadden.blog/2018/08/30/moving-away-from-uuids/
     * 
     * @author james.brown@hydrosolved.com
     */

    private static class RandomString
    {
        private static final SecureRandom random = new SecureRandom();
        private static final Base64.Encoder encoder = Base64.getUrlEncoder().withoutPadding();

        private String generate()
        {
            byte[] buffer = new byte[20];
            random.nextBytes( buffer );
            return encoder.encodeToString( buffer );
        }
    }

    /**
     * Value object for storing a completion status message and a latch to wait until it is available.
     * 
     * @author james.brown@hydrosolved.com
     */

    private class CompletionStatusEvent implements Consumer<EvaluationStatus>
    {

        private final CountDownLatch latch = new CountDownLatch( 1 );

        final AtomicReference<EvaluationStatus> statusMessage = new AtomicReference<>();

        @Override
        public void accept( EvaluationStatus message )
        {
            Objects.requireNonNull( message );

            CompletionStatus status = message.getCompletionStatus();

            if ( status == CompletionStatus.COMPLETE_REPORTED_SUCCESS
                 || status == CompletionStatus.COMPLETE_REPORTED_FAILURE )
            {
                this.statusMessage.set( message );
                this.latch.countDown();
            }
        }

        private void await() throws InterruptedException
        {
            this.latch.await();
        }

        private EvaluationStatus getStatus()
        {
            return this.statusMessage.get();
        }
    }

}
