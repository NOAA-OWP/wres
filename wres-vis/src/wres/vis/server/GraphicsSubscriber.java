package wres.vis.server;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.Topic;
import javax.naming.NamingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

import wres.events.EvaluationEventException;
import wres.eventsbroker.BrokerConnectionFactory;
import wres.statistics.generated.EvaluationStatus;
import wres.statistics.generated.EvaluationStatus.CompletionStatus;
import wres.statistics.generated.Statistics;
import wres.vis.server.GraphicsPublisher.MessageProperty;
import wres.vis.server.GraphicsServer.ServerStatus;
import wres.statistics.generated.Evaluation;

/**
 * Subscribes to evaluation messages and serializes {@link Statistics} to graphics in a prescribed format.
 * 
 * @author james.brown@hydrosolved.com
 */

class GraphicsSubscriber implements Closeable
{

    private static final String EVALUATION = "evaluation {}";

    private static final Logger LOGGER = LoggerFactory.getLogger( GraphicsSubscriber.class );

    private static final String ACKNOWLEDGED_MESSAGE_WITH_CORRELATION_ID =
            "Acknowledged message {} with correlationId {}.";

    private static final String UNKNOWN = "unknown";

    private static final String MESSAGES_WITHIN_GRAPHICS_SUBSCRIBER = "messages within graphics subscriber {}: {}";

    /**
     * If <code>true</code>, do not allow the durable subscriptions to survive graphics server restarts - remove the 
     * subscriptions when closing this subscriber.
     */

    private static final boolean REMOVE_SUBSCRIPTIONS_ON_CLOSURE = false;

    /**
     * A unique identifier for the subscriber.
     */

    private final String uniqueId;

    /**
     * Default name for the queue on the amq.topic that accepts evaluation status messages.
     */

    private static final String EVALUATION_STATUS_QUEUE = "status";

    /**
     * Default name for the queue on the amq.topic that accepts evaluation status messages.
     */

    private static final String EVALUATION_QUEUE = "evaluation";

    /**
     * Default name for the queue on the amq.topic that accepts evaluation status messages.
     */

    private static final String STATISTICS_QUEUE = "statistics";

    /**
     * A publisher for {@link EvaluationStatus} messages.
     */

    private final GraphicsPublisher evaluationStatusPublisher;

    /**
     * Status message consumer.
     */

    private final MessageConsumer evaluationStatusConsumer;

    /**
     * Evaluation message consumer.
     */

    private final MessageConsumer evaluationConsumer;

    /**
     * Statistics message consumer.
     */

    private final MessageConsumer statisticsConsumer;

    /**
     * An evaluation status topic.
     */

    private final Topic evaluationStatusTopic;

    /**
     * An evaluation topic.
     */

    private final Topic evaluationTopic;

    /**
     * A statistics topic.
     */

    private final Topic statisticsTopic;

    /**
     * A session.
     */

    private final Session session;

    /**
     * A consumer connection.
     */

    private final Connection connection;

    /**
     * Broker connections.
     */

    private final BrokerConnectionFactory broker;

    /**
     * Server status.
     */

    private final ServerStatus status;

    /**
     * The evaluations by unique id.
     */

    Map<String, EvaluationConsumer> evaluations = new ConcurrentHashMap<>();

    @Override
    public void close()
    {
        LOGGER.debug( "Closing and then removing subscriptions for {}.",
                      this.getIdentifier() );

        this.closeSubscriptions();

        try
        {
            if ( Objects.nonNull( this.evaluationStatusPublisher ) )
            {
                this.evaluationStatusPublisher.close();
            }
        }
        catch ( IOException e )
        {
            LOGGER.error( "Encountered an error while attempting to close a registered publisher of evaluation status "
                          + MESSAGES_WITHIN_GRAPHICS_SUBSCRIBER,
                          this.getIdentifier(),
                          e.getMessage() );
        }

        try
        {
            this.session.close();
        }
        catch ( JMSException e )
        {
            LOGGER.error( "Encountered an error while attempting to close a broker session within graphics subscriber "
                          + "{}: {}",
                          this.getIdentifier(),
                          e.getMessage() );
        }

        try
        {
            this.connection.close();
        }
        catch ( JMSException e )
        {
            LOGGER.error( "Encountered an error while attempting to close a broker connection within graphics "
                          + "subscriber {}: {}",
                          this.getIdentifier(),
                          e.getMessage() );
        }

        try
        {
            this.broker.close();
        }
        catch ( IOException e )
        {
            LOGGER.error( "Encountered an error while attempting to close a broker connection factory within graphics "
                          + "subscriber {}: {}",
                          this.getIdentifier(),
                          e.getMessage() );
        }
    }

    /**
     * Closes and, where necessary, removes the subscriptions.
     */

    private void closeSubscriptions()
    {
        try
        {
            if ( Objects.nonNull( this.evaluationStatusConsumer ) )
            {
                this.evaluationStatusConsumer.close();
            }
        }
        catch ( JMSException e )
        {
            LOGGER.error( "Encountered an error while attempting to close a registered consumer of evaluation status "
                          + MESSAGES_WITHIN_GRAPHICS_SUBSCRIBER,
                          this.getIdentifier(),
                          e.getMessage() );
        }

        try
        {
            if ( Objects.nonNull( this.evaluationConsumer ) )
            {
                this.evaluationConsumer.close();
            }
        }
        catch ( JMSException e )
        {
            LOGGER.error( "Encountered an error while attempting to close a registered consumer of evaluation "
                          + MESSAGES_WITHIN_GRAPHICS_SUBSCRIBER,
                          this.getIdentifier(),
                          e.getMessage() );
        }

        try
        {
            if ( Objects.nonNull( this.statisticsConsumer ) )
            {
                this.statisticsConsumer.close();
            }
        }
        catch ( JMSException e )
        {
            LOGGER.error( "Encountered an error while attempting to close a registered consumer of statistics "
                          + MESSAGES_WITHIN_GRAPHICS_SUBSCRIBER,
                          this.getIdentifier(),
                          e.getMessage() );
        }

        // Remove durable subscriptions: any messages that arrive when the subscriber is down will be lost
        if ( GraphicsSubscriber.REMOVE_SUBSCRIPTIONS_ON_CLOSURE )
        {
            try
            {
                this.session.unsubscribe( this.getEvaluationStatusSubscriberName() );
            }
            catch ( JMSException e )
            {
                LOGGER.error( "Encountered an error while attempting to remove a durable subscription for evaluation status "
                              + MESSAGES_WITHIN_GRAPHICS_SUBSCRIBER,
                              this.getIdentifier(),
                              e.getMessage() );
            }

            try
            {
                this.session.unsubscribe( this.getEvaluationSubscriberName() );
            }
            catch ( JMSException e )
            {
                LOGGER.error( "Encountered an error while attempting to remove a durable subscription for evaluation "
                              + MESSAGES_WITHIN_GRAPHICS_SUBSCRIBER,
                              this.getIdentifier(),
                              e.getMessage() );
            }

            try
            {
                this.session.unsubscribe( this.getStatisticsSubscriberName() );
            }
            catch ( JMSException e )
            {
                LOGGER.error( "Encountered an error while attempting to remove a durable subscription for statistics "
                              + MESSAGES_WITHIN_GRAPHICS_SUBSCRIBER,
                              this.getIdentifier(),
                              e.getMessage() );
            }
        }
    }

    /**
     * Builds a subscriber.
     * 
     * @param identifier a unique identifier for the subscriber
     * @param status a graphics server whose status can be updated with subscriber progress
     * @throws JMSException if the subscriber components could not be constructed or started.
     * @throws NamingException if the expected broker destinations could not be discovere
     * @throws NullPointerException if any input is null
     */

    GraphicsSubscriber( String uniqueId, ServerStatus status ) throws JMSException, NamingException
    {
        Objects.requireNonNull( uniqueId );
        Objects.requireNonNull( status );

        LOGGER.info( "Building a long-running graphics subscriber {} to listen for evaluation messages...",
                     uniqueId );

        this.uniqueId = uniqueId;
        this.status = status;
        this.broker = BrokerConnectionFactory.of();

        this.evaluationStatusTopic = (Topic) this.broker.getDestination( EVALUATION_STATUS_QUEUE );
        this.evaluationTopic = (Topic) this.broker.getDestination( EVALUATION_QUEUE );
        this.statisticsTopic = (Topic) this.broker.getDestination( STATISTICS_QUEUE );

        this.connection = this.broker.get().createConnection();

        this.evaluationStatusPublisher = GraphicsPublisher.of( this.connection,
                                                               this.evaluationStatusTopic,
                                                               this.getIdentifier() );

        // Create a connection for consumption and register a listener for exceptions
        this.connection.setExceptionListener( new ConnectionExceptionListener() );

        // Client acknowledges
        this.session = this.connection.createSession( false, Session.CLIENT_ACKNOWLEDGE );


        this.evaluationStatusConsumer = this.session.createDurableSubscriber( this.evaluationStatusTopic,
                                                                              this.getEvaluationStatusSubscriberName(),
                                                                              null,
                                                                              false );

        this.evaluationStatusConsumer.setMessageListener( this.getStatusListener() );

        this.evaluationConsumer = this.session.createDurableSubscriber( this.evaluationTopic,
                                                                        this.getEvaluationSubscriberName(),
                                                                        null,
                                                                        false );

        this.evaluationConsumer.setMessageListener( this.getEvaluationListener() );

        this.statisticsConsumer = this.session.createDurableSubscriber( this.statisticsTopic,
                                                                        this.getStatisticsSubscriberName(),
                                                                        null,
                                                                        false );

        this.statisticsConsumer.setMessageListener( this.getStatisticsListener() );

        // Start the consumer connection
        LOGGER.info( "Started the consumer connection for long-running subscriber {}...",
                     this.getIdentifier() );
        this.connection.start();

        LOGGER.info( "Created long-running subscriber {}", this.getIdentifier() );
    }

    /**
     * @return the subscriber identifier.
     */

    private String getIdentifier()
    {
        return this.uniqueId;
    }

    /**
     * @return the name of the durable subscriber to the evaluation status message queue.
     */

    private String getEvaluationStatusSubscriberName()
    {
        return this.getIdentifier() + "-EXTERNAL-status";
    }

    /**
     * @return the name of the durable subscriber to the evaluation message queue.
     */

    private String getEvaluationSubscriberName()
    {
        return this.getIdentifier() + "-EXTERNAL-evaluation";
    }

    /**
     * @return the name of the durable subscriber to the statistics message queue.
     */

    private String getStatisticsSubscriberName()
    {
        return this.getIdentifier() + "-EXTERNAL-statistics";
    }

    /**
     * Awaits evaluation status messages and then consumes them. 
     */

    private MessageListener getStatusListener()
    {
        return message -> {
            BytesMessage receivedBytes = (BytesMessage) message;
            String messageId = UNKNOWN;
            String correlationId = UNKNOWN;
            String consumerId = UNKNOWN;
            try
            {
                messageId = message.getJMSMessageID();
                correlationId = message.getJMSCorrelationID();
                consumerId = message.getStringProperty( MessageProperty.CONSUMER_ID.toString() );

                // Ignore status messages about consumers, including this one
                if ( Objects.isNull( consumerId ) )
                {
                    int messageLength = (int) receivedBytes.getBodyLength();

                    // Create the byte array to hold the message
                    byte[] messageContainer = new byte[messageLength];

                    receivedBytes.readBytes( messageContainer );

                    ByteBuffer buffer = ByteBuffer.wrap( messageContainer );

                    EvaluationStatus statusEvent = EvaluationStatus.parseFrom( buffer );

                    EvaluationConsumer consumer = this.getEvaluationConsumer( correlationId );

                    consumer.acceptStatusMessage( statusEvent, messageId );

                    // Complete?
                    if ( consumer.isComplete() )
                    {
                        consumer.registerComplete();
                    }
                }
                message.acknowledge();

                LOGGER.debug( ACKNOWLEDGED_MESSAGE_WITH_CORRELATION_ID, messageId, correlationId );
            }
            catch ( JMSException | EvaluationEventException | InvalidProtocolBufferException e )
            {
                LOGGER.error( "External subscriber {} failed to consume an evaluation status message {} for "
                              + EVALUATION,
                              this.getIdentifier(),
                              messageId,
                              correlationId );
            }
        };
    }

    /**
     * Awaits evaluation messages and then consumes them. 
     */

    private MessageListener getStatisticsListener()
    {
        return message -> {
            BytesMessage receivedBytes = (BytesMessage) message;
            String messageId = UNKNOWN;
            String correlationId = UNKNOWN;

            try
            {
                messageId = message.getJMSMessageID();
                correlationId = message.getJMSCorrelationID();

                EvaluationConsumer consumer = this.getEvaluationConsumer( correlationId );

                // Create the byte array to hold the message
                int messageLength = (int) receivedBytes.getBodyLength();

                byte[] messageContainer = new byte[messageLength];

                receivedBytes.readBytes( messageContainer );

                ByteBuffer buffer = ByteBuffer.wrap( messageContainer );

                Statistics statistics = Statistics.parseFrom( buffer );

                consumer.acceptStatisticsMessage( statistics, messageId );
                this.status.registerStatistics( messageId );

                // Complete?
                if ( consumer.isComplete() )
                {
                    consumer.registerComplete();
                }

                message.acknowledge();

                LOGGER.debug( ACKNOWLEDGED_MESSAGE_WITH_CORRELATION_ID, messageId, correlationId );
            }
            catch ( JMSException | EvaluationEventException | InvalidProtocolBufferException e )
            {
                this.status.registerFailedEvaluation( correlationId );

                LOGGER.error( "External subscriber {} failed to consume a statistics message {} for "
                              + EVALUATION,
                              this.getIdentifier(),
                              messageId,
                              correlationId );
            }
        };
    }

    /**
     * Awaits evaluation messages and then consumes them. 
     */

    private MessageListener getEvaluationListener()
    {
        return message -> {
            BytesMessage receivedBytes = (BytesMessage) message;
            String messageId = UNKNOWN;
            String correlationId = UNKNOWN;

            try
            {
                messageId = message.getJMSMessageID();
                correlationId = message.getJMSCorrelationID();

                EvaluationConsumer consumer = this.getEvaluationConsumer( correlationId );

                // Create the byte array to hold the message
                int messageLength = (int) receivedBytes.getBodyLength();

                byte[] messageContainer = new byte[messageLength];

                receivedBytes.readBytes( messageContainer );

                ByteBuffer buffer = ByteBuffer.wrap( messageContainer );

                Evaluation evaluation = Evaluation.parseFrom( buffer );

                consumer.acceptEvaluationMessage( evaluation, messageId );

                // Complete?
                if ( consumer.isComplete() )
                {
                    consumer.registerComplete();
                }

                message.acknowledge();

                LOGGER.debug( ACKNOWLEDGED_MESSAGE_WITH_CORRELATION_ID, messageId, correlationId );
            }
            catch ( JMSException | EvaluationEventException | InvalidProtocolBufferException e )
            {
                this.status.registerFailedEvaluation( correlationId );

                LOGGER.error( "External subscriber {} failed to consume an evaluation description message {} for "
                              + EVALUATION,
                              this.getIdentifier(),
                              messageId,
                              correlationId );
            }
        };
    }

    /**
     * Returns a consumer for a prescribed evaluation identifier.
     * @param evaluationId the evaluation identifier
     * @return the consumer
     */

    private EvaluationConsumer getEvaluationConsumer( String evaluationId )
    {
        Objects.requireNonNull( evaluationId,
                                "Cannot request an evaluation consumer for an evaluation with a "
                                              + "missing identifier." );

        // Check initially
        EvaluationConsumer consumer = this.evaluations.get( evaluationId );

        if ( Objects.isNull( consumer ) )
        {
            consumer = new EvaluationConsumer( evaluationId, this.getIdentifier(), this.evaluationStatusPublisher );
            this.status.registerEvaluation( evaluationId );
        }

        // Check atomically
        EvaluationConsumer added = this.evaluations.putIfAbsent( evaluationId, consumer );
        if ( Objects.nonNull( added ) )
        {
            consumer = added;
        }

        return consumer;
    }

    /**
     * Consumer of messages for one evaluation.
     * 
     * @author james.brown@hydrosolved.com
     */

    private class EvaluationConsumer
    {
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

        private final AtomicBoolean registeredComplete;

        /**
         * To notify when consumption complete.
         */

        private final GraphicsPublisher evaluationStatusPublisher;

        /**
         * Builds a consumer.
         * 
         * @param evaluationId the evaluation identifier
         * @param consumerId the consumer identifier
         * @param evaluationStatusPublisher the evaluation status publisher
         * @throws NullPointerException if any input is null
         */

        private EvaluationConsumer( String evaluationId,
                                    String consumerId,
                                    GraphicsPublisher evaluationStatusPublisher )
        {
            Objects.requireNonNull( evaluationId );
            Objects.requireNonNull( consumerId );
            Objects.requireNonNull( evaluationStatusPublisher );

            this.evaluationId = evaluationId;
            this.consumerId = consumerId;
            this.evaluationStatusPublisher = evaluationStatusPublisher;
            this.consumed = new AtomicInteger();
            this.expected = new AtomicInteger();
            this.registeredComplete = new AtomicBoolean();
        }

        private void acceptStatisticsMessage( Statistics statistics, String messageId )
        {
            Objects.requireNonNull( statistics );

            LOGGER.info( "External subscriber {} received and consumed a statistics message with identifier {} "
                         + "for evaluation {}.",
                         this.consumerId,
                         messageId,
                         this.evaluationId );

            this.consumed.incrementAndGet();
        }

        private void acceptEvaluationMessage( Evaluation evaluation, String messageId )
        {
            Objects.requireNonNull( evaluation );

            LOGGER.info( "External subscriber {} received and consumed an evaluation description message with "
                         + "identifier {} for evaluation {}.",
                         this.consumerId,
                         messageId,
                         this.evaluationId );

            this.consumed.incrementAndGet();
        }

        private void acceptStatusMessage( EvaluationStatus status, String messageId )
        {
            Objects.requireNonNull( status );

            LOGGER.info( "External subscriber {} received and consumed an evaluation status message with identifier {} "
                         + "for evaluation {}.",
                         this.consumerId,
                         messageId,
                         this.evaluationId );

            // If publication is complete, then set the expected message count for this evaluation
            if ( status.getCompletionStatus() == CompletionStatus.PUBLICATION_COMPLETE_REPORTED_SUCCESS )
            {
                this.setExpectedMessageCount( status );

                LOGGER.info( "External subscriber {} received notification of publication complete for evaluation {}. "
                             + "The message indicated an expected message count of {}.",
                             this.consumerId,
                             this.evaluationId,
                             this.expected.get() );
            }
        }

        private void setExpectedMessageCount( EvaluationStatus status )
        {
            Objects.requireNonNull( status );

            this.expected.addAndGet( status.getMessageCount() );
        }

        /** 
         * @return true if consumption is complete, otherwise false.
         */

        private boolean isComplete()
        {
            String append = "of an expected message count that is not yet known";
            if ( this.expected.get() > 0 )
            {
                append = "of an expected " + this.expected.get() + " messages";
            }

            LOGGER.info( "For evaluation {}, external subscriber {} has consumed {} messages {}.",
                         this.evaluationId,
                         this.consumerId,
                         this.consumed.get(),
                         append );

            return this.expected.get() > 0 && this.consumed.get() == this.expected.get();
        }

        /**
         * Registers this consumer complete, notifying others that may want to track its progress.
         * @throws JMSException if the completion failed
         */

        private void registerComplete() throws JMSException
        {
            if ( !registeredComplete.get() )
            {
                this.registeredComplete.set( true );

                EvaluationStatus message = EvaluationStatus.newBuilder()
                                                           .setCompletionStatus( CompletionStatus.CONSUMPTION_COMPLETE_REPORTED_SUCCESS )
                                                           .setConsumerId( this.consumerId )
                                                           .build();

                // Create the metadata
                String messageId = "ID:" + this.consumerId + "-complete";

                ByteBuffer buffer = ByteBuffer.wrap( message.toByteArray() );

                this.evaluationStatusPublisher.publish( buffer, messageId, this.evaluationId, this.consumerId );

                LOGGER.debug( "External subscriber {} completed evaluation {}, which contained {} messages.",
                              this.consumerId,
                              this.evaluationId,
                              this.consumed.get() );
            }
        }
    }

    /**
     * Listen for failures on a connection.
     */

    private static class ConnectionExceptionListener implements ExceptionListener
    {

        @Override
        public void onException( JMSException exception )
        {
            exception.printStackTrace();

            LOGGER.error( "Encountered an error on a connection owned by a graphics subscriber: {}.",
                          exception.getMessage() );
        }
    }
}
