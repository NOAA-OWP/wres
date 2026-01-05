package wres.config.serializers;

import java.time.Duration;
import java.util.Objects;

import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.SerializationContext;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.DeclarationUtilities;

/**
 * Serializes a pair of {@link Duration} that represent an interval.
 * @author James Brown
 */
public class DurationIntervalSerializer extends ValueSerializer<Pair<Duration, Duration>>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( DurationIntervalSerializer.class );

    /** The name of the first duration within the interval. */
    private final String firstName;

    /** The name of the second duration within the interval. */
    private final String secondName;

    @Override
    public void serialize( Pair<Duration, Duration> durations, JsonGenerator gen, SerializationContext serializers )
    {
        Objects.requireNonNull( durations );

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Discovered a duration interval of {} to serialize for the {}.",
                          durations,
                          gen.streamWriteContext()
                             .currentName() );
        }

        // Start
        gen.writeStartObject();

        Duration first = durations.getLeft();
        Duration second = durations.getRight();

        // Duration to write?
        if ( Objects.nonNull( first ) || Objects.nonNull( second ) )
        {
            String unit = null;
            if ( Objects.nonNull( first ) )
            {
                Pair<Long, String> serialized = DeclarationUtilities.getDurationInPreferredUnits( first );
                gen.writeNumberProperty( this.firstName, serialized.getLeft() );
                unit = serialized.getRight();
            }

            if ( Objects.nonNull( second ) )
            {
                Pair<Long, String> serialized = DeclarationUtilities.getDurationInPreferredUnits( second );
                gen.writeNumberProperty( this.secondName, serialized.getLeft() );
                unit = serialized.getRight();
            }

            // Units
            gen.writeStringProperty( "unit", unit );
        }

        // End
        gen.writeEndObject();
    }

    /**
     * Creates an instance with the specified names for the duration interval quantities.
     * @param firstName the name of the first duration in context
     * @param secondName the name of the second duration in context
     */
    DurationIntervalSerializer( String firstName, String secondName )
    {
        Objects.requireNonNull( firstName );
        Objects.requireNonNull( secondName );

        this.firstName = firstName;
        this.secondName = secondName;
    }
}
