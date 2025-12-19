package wres.config.serializers;

import java.io.IOException;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.DeclarationUtilities;
import wres.config.components.EventDetectionParameters;

/**
 * Serializes {@link EventDetectionParameters}.
 * @author James Brown
 */
public class EventDetectionParametersSerializer extends JsonSerializer<EventDetectionParameters>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( EventDetectionParametersSerializer.class );

    @Override
    public void serialize( EventDetectionParameters parameters, JsonGenerator gen, SerializerProvider serializers )
            throws IOException
    {
        Objects.requireNonNull( parameters );

        LOGGER.debug( "Discovered event detection parameters {}.", parameters );

        boolean durationWritten = false;

        // Start the parameters object
        gen.writeStartObject();

        if ( Objects.nonNull( parameters.windowSize() ) )
        {
            gen.writeNumberField( "window_size", parameters.windowSize()
                                                           .getSeconds() );
            durationWritten = true;
        }

        if ( Objects.nonNull( parameters.startRadius() ) )
        {
            gen.writeNumberField( "start_radius", parameters.startRadius()
                                                            .getSeconds() );
            durationWritten = true;
        }

        if ( Objects.nonNull( parameters.halfLife() ) )
        {
            gen.writeNumberField( "half_life", parameters.halfLife()
                                                         .getSeconds() );
            durationWritten = true;
        }

        if ( Objects.nonNull( parameters.minimumEventDuration() ) )
        {
            gen.writeNumberField( "minimum_event_duration", parameters.minimumEventDuration()
                                                                      .getSeconds() );
            durationWritten = true;
        }

        if ( Objects.nonNull( parameters.combination() ) )
        {
            // Write the combination and aggregation together
            if ( Objects.nonNull( parameters.aggregation() ) )
            {
                gen.writeFieldName( "combination" );

                gen.writeStartObject();

                String combinationString = DeclarationUtilities.fromEnumName( parameters.combination()
                                                                                        .name() );
                gen.writeStringField( "operation", combinationString );

                String aggregationString = DeclarationUtilities.fromEnumName( parameters.aggregation()
                                                                                        .name() );
                gen.writeStringField( "aggregation", aggregationString );

                gen.writeEndObject();
            }
            // Write the combination alone
            else
            {
                String combinationString = DeclarationUtilities.fromEnumName( parameters.combination()
                                                                                        .name() );
                gen.writeStringField( "combination", combinationString );
            }
        }

        if ( durationWritten )
        {
            gen.writeStringField( "duration_unit", "seconds" );
        }

        // End the parameters object
        gen.writeEndObject();
    }
}
