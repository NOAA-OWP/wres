package wres.eventsbroker;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.naming.NamingException;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests the {@link BrokerConnectionFactory}.
 * 
 * @author james.brown@hydrosolved.com
 */

public class BrokerConnectionFactoryTest
{

    private static final Logger LOGGER = LoggerFactory.getLogger( BrokerConnectionFactoryTest.class );

    @Test
    public void testCreateTwoFactoryInstancesCreatesOneEmbeddedBroker() throws IOException
    {
        // Create and start the broker, clean up on completion
        try ( BrokerConnectionFactory factoryOne = BrokerConnectionFactory.of(); )
        {
            // First factory creates an embedded broker when no other broker is available
            assertTrue( factoryOne.hasEmbeddedBroker() );

            try ( BrokerConnectionFactory factoryTwo = BrokerConnectionFactory.of() )
            {
                // Second factory does not attempt to create a new broker (doing so would produce a connection binding 
                // error on attempting to connect)
                assertFalse( factoryTwo.hasEmbeddedBroker() );
            }
        }
    }

    @Test
    public void testMessageRouting() throws IOException, NamingException, JMSException, InterruptedException
    {
        // Create and start the broker, clean up on completion
        try ( BrokerConnectionFactory factory = BrokerConnectionFactory.of(); )
        {

            Topic evaluationTopic = (Topic) factory.getDestination( "evaluation" );
            Topic evaluationStatusTopic = (Topic) factory.getDestination( "status" );
            Topic statisticsTopic = (Topic) factory.getDestination( "statistics" );

            String evaluationId = "1234567";
            
            // Application/JMS-level message selection based on correlation id
            String messageSelector = "JMSCorrelationID = '" + evaluationId + "'";

            try ( Connection connection = factory.get().createConnection();
                  Session session = connection.createSession( false, Session.AUTO_ACKNOWLEDGE );
                  MessageProducer evaluationProducer = session.createProducer( evaluationTopic );
                  MessageProducer evaluationStatusProducer = session.createProducer( evaluationStatusTopic );
                  MessageProducer statisticsProducer = session.createProducer( statisticsTopic );
                  MessageConsumer evaluationConsumer = session.createConsumer( evaluationTopic, messageSelector );
                  MessageConsumer evaluationStatusConsumer = session.createConsumer( evaluationStatusTopic,
                                                                                     messageSelector );
                  MessageConsumer statisticsConsumer = session.createConsumer( statisticsTopic, messageSelector ); )
            {
                // Latches to identify when consumption is complete
                CountDownLatch evaluationConsumerCount = new CountDownLatch( 1 );
                CountDownLatch evaluationStatusConsumerCount = new CountDownLatch( 1 );
                CountDownLatch statisticsConsumerCount = new CountDownLatch( 1 );

                // Listen for evaluation messages
                MessageListener evaluationListener = message -> {
                    TextMessage textMessage = (TextMessage) message;
                    
                    try
                    {
                        assertEquals( "I am an evaluation message!", textMessage.getText() );
                    }
                    catch ( JMSException e )
                    {
                        throw new IllegalStateException( e );
                    }
                    
                    LOGGER.info( "Received an evaluation message {}", textMessage );
                    evaluationConsumerCount.countDown();
                };

                // Listen for evaluation status messages
                MessageListener evaluationStatusListener = message -> {
                    TextMessage textMessage = (TextMessage) message;
                    
                    try
                    {
                        assertEquals( "I am an evaluation status message!", textMessage.getText() );
                    }
                    catch ( JMSException e )
                    {
                        throw new IllegalStateException( e );
                    }
                    
                    LOGGER.info( "Received an evaluation status message {}", textMessage );
                    evaluationStatusConsumerCount.countDown();
                };

                // Listen for statistics messages
                MessageListener evaluationStatisticsListener = message -> {
                    TextMessage textMessage = (TextMessage) message;
                    
                    try
                    {
                        assertEquals( "I am a statistics message!", textMessage.getText() );
                    }
                    catch ( JMSException e )
                    {
                        throw new IllegalStateException( e );
                    }
                    
                    LOGGER.info( "Received a statistics message {}", textMessage );
                    statisticsConsumerCount.countDown();
                };

                // Start the consumer connection 
                connection.start();

                // Subscribe the listener to the consumer
                evaluationConsumer.setMessageListener( evaluationListener );
                evaluationStatusConsumer.setMessageListener( evaluationStatusListener );
                statisticsConsumer.setMessageListener( evaluationStatisticsListener );

                // Publish some messages
                TextMessage evaluationMessage = session.createTextMessage( "I am an evaluation message!" );
                TextMessage evaluationStatusMessage = session.createTextMessage( "I am an evaluation status message!" );
                TextMessage statisticsMessage = session.createTextMessage( "I am a statistics message!" );
                evaluationMessage.setJMSCorrelationID( evaluationId );
                evaluationStatusMessage.setJMSCorrelationID( evaluationId );
                statisticsMessage.setJMSCorrelationID( evaluationId );

                evaluationProducer.send( evaluationMessage );
                evaluationStatusProducer.send( evaluationStatusMessage );
                statisticsProducer.send( statisticsMessage );

                // Await the sooner of all messages read and a timeout
                boolean done = evaluationConsumerCount.await( 2000L, TimeUnit.MILLISECONDS )
                               && evaluationStatusConsumerCount.await( 2000L, TimeUnit.MILLISECONDS )
                               && statisticsConsumerCount.await( 2000L, TimeUnit.MILLISECONDS );

                assertTrue( done );
            }
        }
    }


}
