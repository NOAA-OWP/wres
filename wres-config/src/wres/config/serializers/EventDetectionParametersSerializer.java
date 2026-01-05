package wres.config.serializers;

import java.util.Objects;

import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.SerializationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.DeclarationUtilities;
import wres.config.components.EventDetectionParameters;

/**
 * Serializes {@link EventDetectionParameters}.
 * @author James Brown
 */
public class EventDetectionParametersSerializer extends ValueSerializer<EventDetectionParameters>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( EventDetectionParametersSerializer.class );

    @Override
    public void serialize( EventDetectionParameters parameters, JsonGenerator gen, SerializationContext serializers )
    {
        Objects.requireNonNull( parameters );

        LOGGER.debug( "Discovered event detection parameters {}.", parameters );

        boolean durationWritten = false;

        // Start the parameters object
        gen.writeStartObject();

        if ( Objects.nonNull( parameters.windowSize() ) )
        {
            gen.writeNumberProperty( "window_size", parameters.windowSize()
                                                              .getSeconds() );
            durationWritten = true;
        }

        if ( Objects.nonNull( parameters.startRadius() ) )
        {
            gen.writeNumberProperty( "start_radius", parameters.startRadius()
                                                               .getSeconds() );
            durationWritten = true;
        }

        if ( Objects.nonNull( parameters.halfLife() ) )
        {
            gen.writeNumberProperty( "half_life", parameters.halfLife()
                                                            .getSeconds() );
            durationWritten = true;
        }

        if ( Objects.nonNull( parameters.minimumEventDuration() ) )
        {
            gen.writeNumberProperty( "minimum_event_duration", parameters.minimumEventDuration()
                                                                         .getSeconds() );
            durationWritten = true;
        }

        if ( Objects.nonNull( parameters.combination() ) )
        {
            // Write the combination and aggregation together
            if ( Objects.nonNull( parameters.aggregation() ) )
            {
                gen.writeName( "combination" );

                gen.writeStartObject();

                String combinationString = DeclarationUtilities.fromEnumName( parameters.combination()
                                                                                        .name() );
                gen.writeStringProperty( "operation", combinationString );

                String aggregationString = DeclarationUtilities.fromEnumName( parameters.aggregation()
                                                                                        .name() );
                gen.writeStringProperty( "aggregation", aggregationString );

                gen.writeEndObject();
            }
            // Write the combination alone
            else
            {
                String combinationString = DeclarationUtilities.fromEnumName( parameters.combination()
                                                                                        .name() );
                gen.writeStringProperty( "combination", combinationString );
            }
        }

        if ( durationWritten )
        {
            gen.writeStringProperty( "duration_unit", "seconds" );
        }

        // End the parameters object
        gen.writeEndObject();
    }
}
