package wres.events;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Phaser;
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

public class Evaluation implements Closeable
{

    private static final Logger LOGGER = LoggerFactory.getLogger( Evaluation.class );

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
     * Repeated error message on failing to find subscribers.
     */

    private static final String CANNOT_BUILD_AN_EVALUATION_WITHOUT_ONE_OR_MORE_SUBSCRIBERS =
            "Cannot build an evaluation without one or more subscribers for ";

    /**
     * Repeated error message on mapping from a bytebuffer to a message.
     */

    private static final String MESSAGE_FROM_A_BYTEBUFFER = " message from a bytebuffer.";

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

    private final MessageSubscriber evaluationSubscribers;

    /**
     * A collection of subscribers for {@link wres.statistics.generated.Statistics} messages.
     */

    private final MessageSubscriber statisticsSubscribers;

    /**
     * A collection of subscribers for {@link EvaluationStatus} messages.
     */

    private final MessageSubscriber evaluationStatusSubscribers;

    /**
     * A unique identifier for the evaluation.
     */

    private final String evaluationId;

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
     * @param consumerGroup the consumers to subscribe
     * @return an open evaluation
     * @throws NullPointerException if any input is null
     */

    public static Evaluation open( wres.statistics.generated.Evaluation evaluation,
                                   BrokerConnectionFactory broker,
                                   ConsumerGroup consumerGroup )
    {
        Objects.requireNonNull( evaluation );
        Objects.requireNonNull( broker );
        Objects.requireNonNull( consumerGroup );

        Builder builder = new Builder();
        builder.setBroker( broker )
               .setEvaluation( evaluation );
        consumerGroup.getEvaluationStatusConsumers().forEach( builder::addEvaluationStatusSubscriber );
        consumerGroup.getEvaluationConsumers().forEach( builder::addEvaluationSubscriber );
        consumerGroup.getStatisticsConsumers().forEach( builder::addStatisticsSubscriber );

        return builder.build();
    }

    /**
     * Publish an {@link wres.statistics.generated.EvaluationStatus} message for the current evaluation.
     * 
     * @param status the status message
     */

    public void publish( EvaluationStatus status )
    {
        Objects.requireNonNull( status );

        ByteBuffer body = ByteBuffer.wrap( status.toByteArray() );
        

        // Provide a hint to the application to await consumption on closing an evaluation
        this.evaluationStatusSubscribers.advanceCountToAwaitOnClose();
        
        this.internalPublish( body, this.evaluationStatusPublisher, Evaluation.EVALUATION_STATUS_QUEUE );
    }

    /**
     * Publish an {@link wres.statistics.generated.Statistics} message for the current evaluation.
     * 
     * @param statistics the statistics message
     */

    public void publish( Statistics statistics )
    {
        Objects.requireNonNull( statistics );

        ByteBuffer body = ByteBuffer.wrap( statistics.toByteArray() );
        
        // Provide a hint to the application to await consumption on closing an evaluation
        this.statisticsSubscribers.advanceCountToAwaitOnClose();

        this.internalPublish( body, this.statisticsPublisher, Evaluation.STATISTICS_QUEUE );

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
        
        this.publish( ongoing );
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

        // Close gracefully, once consumption has completed
        Phaser statisticsPhaser = this.statisticsSubscribers.getStatus();
        Phaser statusPhaser = this.evaluationStatusSubscribers.getStatus();
        Phaser evaluationPhaser = this.evaluationSubscribers.getStatus();

        LOGGER.debug( "While closing evaluation {}, found {} evaluation messages, {} statistics messages and {} "
                      + "evaluation status messages awaiting consumption...",
                      this.getEvaluationId(),
                      evaluationPhaser.getRegisteredParties(),
                      statisticsPhaser.getRegisteredParties(),
                      statusPhaser.getRegisteredParties() );

        // Wait for enqueued messages in the absence of failure. Wait for status messages later, as there is
        // one more to publish on success
        statisticsPhaser.awaitAdvance( 0 );
        evaluationPhaser.awaitAdvance( 0 );

        Instant now = Instant.now();
        long seconds = now.getEpochSecond();
        int nanos = now.getNano();

        // Failure?
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

        // Success
        EvaluationStatus ongoing = EvaluationStatus.newBuilder()
                                                   .setCompletionStatus( CompletionStatus.COMPLETE_REPORTED_SUCCESS )
                                                   .setEvaluationEndTime( Timestamp.newBuilder()
                                                                                   .setSeconds( seconds )
                                                                                   .setNanos( nanos ) )
                                                   .build();

        this.publish( ongoing );
        
        // Await consumption of the final status message
        statusPhaser.awaitAdvance( 0 );
        
        this.evaluationPublisher.close();
        this.evaluationSubscribers.close();
        this.evaluationStatusPublisher.close();
        this.evaluationStatusSubscribers.close();
        this.statisticsPublisher.close();
        this.statisticsSubscribers.close();

        LOGGER.debug( "Closed evaluation {}.", this.getEvaluationId() );
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
         * List of subscriptions to evaluation status events.
         */

        private List<Consumer<EvaluationStatus>> statusConsumers = new ArrayList<>();

        /**
         * List of subscriptions to evaluation events.
         */

        private List<Consumer<wres.statistics.generated.Evaluation>> evaluationConsumers = new ArrayList<>();

        /**
         * List of subscriptions to statistics events.
         */

        private List<Consumer<Statistics>> statisticsConsumers = new ArrayList<>();

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
         * Adds a subscription to evaluation status events.
         * 
         * @param subscriber the consumer subscription
         * @return this builder 
         */

        public Builder addEvaluationStatusSubscriber( Consumer<EvaluationStatus> subscriber )
        {
            this.statusConsumers.add( subscriber );

            return this;
        }

        /**
         * Adds a subscription to evaluation events.
         * 
         * @param subscriber the consumer subscription
         * @return this builder
         */

        public Builder addEvaluationSubscriber( Consumer<wres.statistics.generated.Evaluation> subscriber )
        {
            this.evaluationConsumers.add( subscriber );

            return this;
        }

        /**
         * Adds a subscription to statistics events.
         * 
         * @param subscriber the consumer subscription
         * @return this builder
         */

        public Builder addStatisticsSubscriber( Consumer<Statistics> subscriber )
        {
            this.statisticsConsumers.add( subscriber );

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
        // Set then validate
        BrokerConnectionFactory broker = builder.broker;
        List<Consumer<wres.statistics.generated.Evaluation>> evaluationSubs =
                new ArrayList<>( builder.evaluationConsumers );
        List<Consumer<EvaluationStatus>> statusSubs = new ArrayList<>( builder.statusConsumers );
        List<Consumer<Statistics>> statisticsSubs = new ArrayList<>( builder.statisticsConsumers );
        wres.statistics.generated.Evaluation evaluationMessage = builder.evaluation;
        Objects.requireNonNull( builder.broker, "Cannot create an evaluation without a broker connection." );
        Objects.requireNonNull( builder.evaluation, "Cannot create an evaluation without an evaluation message." );

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

        if ( statisticsSubs.isEmpty() )
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
                                                                     .setMapper( this.getStatusMapper() )
                                                                     .setEvaluationId( this.getEvaluationId() )
                                                                     .build();

            Destination evaluation = broker.getDestination( Evaluation.EVALUATION_QUEUE );
            this.evaluationPublisher = MessagePublisher.of( factory, evaluation );
            this.evaluationSubscribers =
                    new MessageSubscriber.Builder<wres.statistics.generated.Evaluation>().setConnectionFactory( factory )
                                                                                         .setDestination( evaluation )
                                                                                         .addSubscribers( evaluationSubs )
                                                                                         .setMapper( this.getEvaluationMapper() )
                                                                                         .setEvaluationId( this.getEvaluationId() )
                                                                                         .build();

            Destination statistics = broker.getDestination( Evaluation.STATISTICS_QUEUE );
            this.statisticsPublisher = MessagePublisher.of( factory, statistics );
            this.statisticsSubscribers =
                    new MessageSubscriber.Builder<Statistics>().setConnectionFactory( factory )
                                                               .setDestination( statistics )
                                                               .addSubscribers( statisticsSubs )
                                                               .setMapper( this.getStatisticsMapper() )
                                                               .setEvaluationId( this.getEvaluationId() )
                                                               .build();
        }
        catch ( JMSException | NamingException e )
        {
            throw new EvaluationEventException( "Unable to construct an evaluation.", e );
        }

        LOGGER.info( "Created a new evaluation with id {}.", evaluationId );

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
     */

    private void internalPublish( ByteBuffer body, MessagePublisher publisher, String queue )
    {

        // Published below, so increment by 1 here 
        String messageId = "ID:" + this.getEvaluationId() + "-m" + ( publisher.getMessageCount() + 1 );
        
        try
        {
            publisher.publish( body, messageId, this.getEvaluationId() );

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
        
        // Provide a hint to the application to await consumption on closing an evaluation
        this.evaluationSubscribers.advanceCountToAwaitOnClose();

        this.internalPublish( body, this.evaluationPublisher, Evaluation.EVALUATION_QUEUE );
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

}
