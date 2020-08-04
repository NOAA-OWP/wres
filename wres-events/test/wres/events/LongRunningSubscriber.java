package wres.events;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.EnumMap;
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

import wres.events.MessagePublisher.MessageProperty;
import wres.eventsbroker.BrokerConnectionFactory;
import wres.statistics.generated.EvaluationStatus;
import wres.statistics.generated.EvaluationStatus.CompletionStatus;
import wres.statistics.generated.Statistics;
import wres.statistics.generated.Evaluation;


/**
 * Simulates a long-running subscription to evaluation messages from an amqp message broker. Start this subscriber by
 * calling the main method. This is for out-of-band testing of external subscriptions and messaging with a long-running 
 * broker instance.
 * 
 * @author james.brown@hydrosolved.com
 */

class LongRunningSubscriber
{

    private static final Logger LOGGER = LoggerFactory.getLogger( LongRunningSubscriber.class );

    private static final String ACKNOWLEDGED_MESSAGE_WITH_CORRELATION_ID =
            "Acknowledged message {} with correlationId {}.";

    private static final String UNKNOWN = "unknown";


    /**
     * A unique identifier for the subscriber.
     */

    private static final String UNIQUE_ID = "4mOgkGkse3gWIGKuIhzVnl5ZPCM";

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

    private final MessagePublisher evaluationStatusPublisher;

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

    private final Connection consumerConnection;

    /**
     * The evaluations by unique id.
     */

    Map<String, EvaluationConsumer> evaluations = new ConcurrentHashMap<>();

    /**
     * Create the long-running subscriber.
     * 
     * @param args input args
     * @throws InterruptedException if the subscriber is interrupted
     * @throws NamingException if the message destination could not be found
     * @throws JMSException if the subscriber could not be created for any other reason
     */

    public static void main( String[] args ) throws InterruptedException, JMSException, NamingException
    {
        new LongRunningSubscriber();

        // Keep alive indefinitely
        Thread.currentThread().join();
    }

    private LongRunningSubscriber() throws JMSException, NamingException
    {
        LOGGER.info( "Creating long-running subscriber {} to listen for evaluation messages...",
                     LongRunningSubscriber.UNIQUE_ID );

        BrokerConnectionFactory factory = BrokerConnectionFactory.of();

        this.evaluationStatusTopic = (Topic) factory.getDestination( EVALUATION_STATUS_QUEUE );
        this.evaluationTopic = (Topic) factory.getDestination( EVALUATION_QUEUE );
        this.statisticsTopic = (Topic) factory.getDestination( STATISTICS_QUEUE );

        this.consumerConnection = factory.get().createConnection();
        
        this.evaluationStatusPublisher = MessagePublisher.of( this.consumerConnection, this.evaluationStatusTopic );

        // Create a connection for consumption and register a listener for exceptions
        this.consumerConnection.setExceptionListener( new EvaluationEventExceptionListener() );

        // Client acknowledges
        this.session = this.consumerConnection.createSession( false, Session.CLIENT_ACKNOWLEDGE );


        this.evaluationStatusConsumer = this.session.createDurableSubscriber( this.evaluationStatusTopic,
                                                                              LongRunningSubscriber.UNIQUE_ID
                                                                                                          + "-EXTERNAL-status",
                                                                              null,
                                                                              false );

        this.evaluationStatusConsumer.setMessageListener( this.getStatusListener() );

        this.evaluationConsumer = this.session.createDurableSubscriber( this.evaluationTopic,
                                                                        LongRunningSubscriber.UNIQUE_ID
                                                                                              + "-EXTERNAL-evaluation",
                                                                        null,
                                                                        false );

        this.evaluationConsumer.setMessageListener( this.getEvaluationListener() );

        this.statisticsConsumer = this.session.createDurableSubscriber( this.statisticsTopic,
                                                                        LongRunningSubscriber.UNIQUE_ID
                                                                                              + "-EXTERNAL-statistics",
                                                                        null,
                                                                        false );

        this.statisticsConsumer.setMessageListener( this.getStatisticsListener() );

        // Start the consumer connection
        LOGGER.info( "Started the consumer connection for long-running subscriber {}...",
                     LongRunningSubscriber.UNIQUE_ID );
        this.consumerConnection.start();

        LOGGER.info( "Created long-running subscriber {}. Waiting for messages...", LongRunningSubscriber.UNIQUE_ID );
    }

    /**
     * Awaits evaluation messages and then consumes them. 
     */

    private MessageListener getStatusListener()
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

                EvaluationStatus status = EvaluationStatus.parseFrom( buffer );

                consumer.acceptStatusMessage( status, messageId );

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
                LOGGER.error( "External subscriber {} failed to consume an evaluation status message {} for "
                              + "evaluation {}",
                              LongRunningSubscriber.UNIQUE_ID,
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
                LOGGER.error( "External subscriber {} failed to consume an evaluation description message {} for "
                              + "evaluation {}",
                              LongRunningSubscriber.UNIQUE_ID,
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
                LOGGER.error( "External subscriber {} failed to consume a statistics message {} for evaluation {}",
                              LongRunningSubscriber.UNIQUE_ID,
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
        // Check initially
        EvaluationConsumer consumer = this.evaluations.get( evaluationId );

        if ( Objects.isNull( consumer ) )
        {
            consumer = new EvaluationConsumer( evaluationId, this.evaluationStatusPublisher );
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
     * Evaluation consumer.
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
         * Actual number of messages consumed.
         */

        private final AtomicInteger consumed;

        /**
         * Expected number of messages consumed.
         */

        private final AtomicInteger expected;

        /**
         * Registered complete.
         */

        private final AtomicBoolean registeredComplete;

        /**
         * To notify when consumption complete.
         */

        private final MessagePublisher evaluationStatusPublisher;

        private EvaluationConsumer( String evaluationId, MessagePublisher evaluationStatusPublisher )
        {
            Objects.requireNonNull( evaluationId );
            Objects.requireNonNull( evaluationStatusPublisher );

            this.evaluationId = evaluationId;
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
                         LongRunningSubscriber.UNIQUE_ID,
                         messageId,
                         this.evaluationId );

            this.consumed.incrementAndGet();
        }

        private void acceptEvaluationMessage( Evaluation evaluation, String messageId )
        {
            Objects.requireNonNull( evaluation );

            LOGGER.info( "External subscriber {} received and consumed an evaluation description message with "
                         + "identifier {} for evaluation {}.",
                         LongRunningSubscriber.UNIQUE_ID,
                         messageId,
                         this.evaluationId );

            this.consumed.incrementAndGet();
        }

        private void acceptStatusMessage( EvaluationStatus status, String messageId )
        {
            Objects.requireNonNull( status );

            LOGGER.info( "External subscriber {} received and consumed an evaluation status message with identifier {} "
                         + "for evaluation {}.",
                         LongRunningSubscriber.UNIQUE_ID,
                         messageId,
                         this.evaluationId );

            // If publication is complete, then set the expected message count for this evaluation
            if ( status.getCompletionStatus() == CompletionStatus.PUBLICATION_COMPLETE_REPORTED_SUCCESS )
            {
                this.setExpectedMessageCount( status );

                LOGGER.info( "External subscriber {} received notification of publication complete for evaluation {}. "
                             + "The message indicated an expected message count of {}.",
                             LongRunningSubscriber.UNIQUE_ID,
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
            LOGGER.info( "External subscriber {} has consumed {} messages of an expected {} messages.",
                         LongRunningSubscriber.UNIQUE_ID,
                         this.consumed.get(),
                         this.expected.get() );

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
                                                           .setConsumerId( LongRunningSubscriber.UNIQUE_ID )
                                                           .build();

                // Create the metadata
                Map<MessageProperty, String> properties = new EnumMap<>( MessageProperty.class );
                properties.put( MessageProperty.JMS_MESSAGE_ID, "ID:" + LongRunningSubscriber.UNIQUE_ID + "-complete" );
                properties.put( MessageProperty.JMS_CORRELATION_ID, this.evaluationId );
                properties.put( MessageProperty.CONSUMER_ID, LongRunningSubscriber.UNIQUE_ID );

                ByteBuffer buffer = ByteBuffer.wrap( message.toByteArray() );

                this.evaluationStatusPublisher.publish( buffer, Collections.unmodifiableMap( properties ) );

                LOGGER.debug( "External subscriber {} completed evaluation {}, which contained {} messages.",
                              LongRunningSubscriber.UNIQUE_ID,
                              this.evaluationId,
                              this.consumed.get() );
            }
        }
    }

    /**
     * Listen for failures on a connection.
     */

    private static class EvaluationEventExceptionListener implements ExceptionListener
    {

        @Override
        public void onException( JMSException exception )
        {
            throw new EvaluationEventException( "Encountered an error while attempting to complete an evaluation "
                                                + "message.",
                                                exception );
        }
    }

}
