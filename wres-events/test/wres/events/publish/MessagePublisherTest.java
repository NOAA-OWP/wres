package wres.events.publish;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.jms.BytesMessage;
import jakarta.jms.Connection;
import jakarta.jms.DeliveryMode;
import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import javax.naming.NamingException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import wres.events.broker.BrokerConnectionFactory;
import wres.events.broker.BrokerUtilities;
import wres.events.publish.MessagePublisher.MessageProperty;
import wres.eventsbroker.embedded.EmbeddedBroker;

/**
 * Tests the {@link MessagePublisher}
 * 
 * @author James Brown
 */

class MessagePublisherTest
{
    /**
     * Embedded broker.
     */

    private static EmbeddedBroker broker = null;

    /**
     * Broker connection factory.
     */

    private static BrokerConnectionFactory connections = null;

    @BeforeAll
    static void runBeforeAllTests()
    {
        // Create and start and embedded broker
        Properties properties = BrokerUtilities.getBrokerConnectionProperties( "eventbroker.properties" );
        MessagePublisherTest.broker = EmbeddedBroker.of( properties, true );
        MessagePublisherTest.broker.start();
        
        // Create a connection factory to supply broker connections
        MessagePublisherTest.connections = BrokerConnectionFactory.of( properties, 2 );
    }

    @Test
    void publishOneMessage() throws IOException, NamingException, JMSException
    {
        // Create and start a broker and open an evaluation, closing on completion
        try ( MessagePublisher publisher =
                MessagePublisher.of( MessagePublisherTest.connections,
                                     MessagePublisherTest.connections.getDestination( "status" ) );
              Connection consumerConnection = MessagePublisherTest.connections.get();
              Session session = consumerConnection.createSession( Session.AUTO_ACKNOWLEDGE );
              MessageConsumer consumer =
                      session.createConsumer( MessagePublisherTest.connections.getDestination( "status" ) ) )
        {
            publisher.start();

            consumerConnection.start();

            Map<MessageProperty, String> properties = new EnumMap<>( MessageProperty.class );
            properties.put( MessageProperty.JMS_MESSAGE_ID, "ID:someId" );
            properties.put( MessageProperty.JMS_CORRELATION_ID, "someCorrelationId" );
            properties.put( MessageProperty.JMSX_GROUP_ID, "aGroupId" );

            publisher.publish( ByteBuffer.wrap( "some bytes".getBytes() ), Collections.unmodifiableMap( properties ) );

            // Blocking wait
            BytesMessage received = (BytesMessage) consumer.receive();

            // Create the byte array to hold the message
            int messageLength = (int) received.getBodyLength();
            byte[] messageContainer = new byte[messageLength];
            received.readBytes( messageContainer );
            String body = new String( messageContainer );

            assertEquals( "some bytes", body );
        }
    }

    @Test
    void publishOneMessageSucceedsAfterOneRetry() throws IOException, NamingException, JMSException
    {
        // Goal: mock a MessageProducer whose send method needs to be invoked
        BrokerConnectionFactory mockFactory = Mockito.mock( BrokerConnectionFactory.class );

        Mockito.when( mockFactory.getMaximumMessageRetries() )
               .thenReturn( 1 );

        Connection mockConnection = Mockito.mock( Connection.class );
        Mockito.when( mockFactory.get() )
               .thenReturn( mockConnection );

        Session mockSession = Mockito.mock( Session.class );
        Mockito.when( mockConnection.createSession( Session.CLIENT_ACKNOWLEDGE ) )
               .thenReturn( mockSession );

        MessageProducer mockProducer = Mockito.mock( MessageProducer.class );
        Destination statusDestination = MessagePublisherTest.connections.getDestination( "status" );

        Mockito.when( mockSession.createProducer( statusDestination ) )
               .thenReturn( mockProducer );

        Connection realConnection = MessagePublisherTest.connections.get();
        Session realSession = realConnection.createSession( Session.AUTO_ACKNOWLEDGE );

        // Create an answer that records an actual message when MessageProducer::send is invoked
        AtomicBoolean invokedAsExpected = new AtomicBoolean();
        Answer<Object> answer = invocation -> {
            invokedAsExpected.set( true );
            return null;
        };

        BytesMessage bytesMessage = realSession.createBytesMessage();
        bytesMessage.writeBytes( "some bytes".getBytes() );

        // When send is successfully called, then answer
        Mockito.doAnswer( answer )
               .when( mockProducer )
               .send( bytesMessage,
                      DeliveryMode.NON_PERSISTENT,
                      Message.DEFAULT_PRIORITY,
                      Message.DEFAULT_TIME_TO_LIVE );

        // The message producer needs to create a bytes message and does so within a retry loop, so use this as a hook
        // to test the retry behavior. On the first call, throw an exception and on the second call return a real 
        // message, which is then sent by the mocked producer. 
        // This is indirect/ugly insofar as it relies on an implementation detail of the retry loop, namely
        // that the createBytesMessage happens within it.
        Mockito.when( mockSession.createBytesMessage() )
               .thenThrow( new JMSException( "an exception" ) )
               .thenReturn( bytesMessage );

        try ( MessagePublisher publisher =
                MessagePublisher.of( mockFactory,
                                     statusDestination ) )
        {
            publisher.start();

            Map<MessageProperty, String> properties = new EnumMap<>( MessageProperty.class );
            properties.put( MessageProperty.JMS_MESSAGE_ID, "ID:someId" );
            properties.put( MessageProperty.JMS_CORRELATION_ID, "someCorrelationId" );
            properties.put( MessageProperty.JMSX_GROUP_ID, "aGroupId" );

            // Publish a message that generates a JMSException initially, then succeeds on first retry.
            // If the test choreography fails for any reason, then this method can be expected to throw an 
            // UnrecoverablePublisherException after all retries.
            publisher.publish( ByteBuffer.wrap( "some bytes".getBytes() ), Collections.unmodifiableMap( properties ) );
        }
        finally
        {
            // No need to close session etc.
            realConnection.close();
        }

        assertTrue( invokedAsExpected.get() );
    }

    @AfterAll
    static void runAfterAllTests() throws IOException
    {
        if ( Objects.nonNull( MessagePublisherTest.broker ) )
        {
            MessagePublisherTest.broker.close();
        }
    }

}
