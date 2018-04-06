package wres.tasker;

import java.io.IOException;
import java.util.StringJoiner;
import java.util.concurrent.TimeoutException;
import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path( "/job")
public class WresJob
{
    private static final Logger LOGGER = LoggerFactory.getLogger( WresJob.class );

    private static final String SEND_QUEUE_NAME = "wres.job";

    private final ConnectionFactory connectionFactory;

    WresJob()
    {
        this.connectionFactory = new ConnectionFactory();
        this.connectionFactory.setHost( "localhost" );
    }

    @POST
    @Consumes( "application/x-www-form-urlencoded")
    @Produces( "text/html")
    public Response postWresJob( @FormParam( "projectPath" ) String projectPath,
                                 @FormParam( "databaseUrl" ) String databaseUrl,
                                 @FormParam( "databaseName" ) String databaseName,
                                 @FormParam( "databaseUser" ) String databaseUser,
                                 @FormParam( "databasePassword" ) String databasePassword )
    {
        StringJoiner messageBuilder = new StringJoiner( "," );
        messageBuilder.add( "projectConfig=" + projectPath );
        messageBuilder.add( "JAVA_OPTS=-Dwres.url=" + databaseUrl + " "
                            + "-Dwres.databaseName=" + databaseName + " "
                            + "-Dwres.username=" + databaseUser + " "
                            + "-Dwres.password=" + databasePassword );

        try
        {
            sendMessage( messageBuilder.toString() );
        }
        catch ( IOException | TimeoutException e )
        {
            LOGGER.error( "Attempt to send message failed.", e );
            return this.internalServerError();
        }

        return Response.ok( "<!DOCTYPE html><html><head><title>Job received.</title></head><body><h1>Your job has been received for processing.</h1></body></html>" )
                       .build();
    }

    private void sendMessage( String message )
            throws IOException, TimeoutException
    {
        try ( Connection connection = this.connectionFactory.newConnection();
              Channel channel = connection.createChannel() )
        {
            channel.queueDeclare( SEND_QUEUE_NAME, false, false, false, null );

                channel.basicPublish( "",
                                      SEND_QUEUE_NAME,
                                      null,
                                      message.getBytes() );
            if ( LOGGER.isInfoEnabled() )
            {
                LOGGER.info( "I sent this message to the queue.", message );
            }
        }
    }

    private Response internalServerError()
    {
        return Response.serverError()
                       .entity("<!DOCTYPE html><html><head><title>Our mistake</title></head><body><h1>Internal Server Error</h1><p>An issue occurred that is not your fault.</p></body></html>")
                       .build();
    }
}
