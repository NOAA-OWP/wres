package wres.events;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Objects;

import wres.statistics.generated.EvaluationStatus.EvaluationStatusEvent;
import wres.statistics.generated.EvaluationStatus.EvaluationStatusEvent.StatusMessageType;

/**
 * A utility class to help with the messaging of evaluations.
 * 
 * @author james.brown@hydrosolved.com
 */

public class EvaluationEventUtilities
{

    /**
     * Used to generate unique evaluation identifiers.
     */

    private static final RandomString ID_GENERATOR = new RandomString();

    /**
     * Returns a unique identifier for identifying a component of an evaluation, such as the evaluation itself or 
     * messaging client.
     * 
     * @return a unique identifier
     */

    public static String getUniqueId()
    {
        return EvaluationEventUtilities.ID_GENERATOR.generate();
    }

    /**
     * Creates an {@link EvaluationStatusEvent} from an exception in order to message an error.
     * 
     * @param exception the exception
     * @return the exception event
     * @throws NullPointerException if the exception is null
     */

    public static EvaluationStatusEvent getStatusEventFromException( Exception exception )
    {
        Objects.requireNonNull( exception );

        EvaluationStatusEvent.Builder event = EvaluationStatusEvent.newBuilder()
                                                                   .setEventType( StatusMessageType.ERROR )
                                                                   .setEventMessage( exception.getClass() + ": "
                                                                                     + exception.getMessage() );

        // Add up to five causes, where available
        Throwable cause = exception.getCause();
        List<EvaluationStatusEvent.Builder> causes = new ArrayList<>();
        for ( int i = 0; i < 5; i++ )
        {
            if ( Objects.nonNull( cause ) )
            {
                String causeClass = cause.getClass() + ": ";
                String message = "";

                if ( Objects.nonNull( cause.getMessage() ) )
                {
                    message = cause.getMessage();
                }
                
                String eventMessage = causeClass + message;
                
                EvaluationStatusEvent.Builder causeEvent = EvaluationStatusEvent.newBuilder()
                                                                                .setEventType( StatusMessageType.ERROR )
                                                                                .setEventMessage( eventMessage );
                causes.add( causeEvent );
            }
            else
            {
                break;
            }

            cause = cause.getCause();
        }

        // Add the causes in reverse order so that they propagate up the build
        if ( !causes.isEmpty() )
        {
            for ( int i = causes.size() - 1; i > 0; i-- )
            {
                causes.get( i - 1 )
                      .setCause( causes.get( i ) );
            }

            event.setCause( causes.get( 0 ) );
        }

        return event.build();
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
     * Do not construct.
     */
    
    private EvaluationEventUtilities()
    {
    }

}
