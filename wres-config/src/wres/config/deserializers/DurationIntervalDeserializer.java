package wres.config.deserializers;

import java.time.Duration;
import java.util.Objects;

import tools.jackson.core.JsonParser;
import tools.jackson.core.ObjectReadContext;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.JsonNode;

import wres.config.DeclarationFactory;

/**
 * Custom deserializer for a pair of {@link Duration} that represents an interval, together with an indicator about
 * whether the interval is forwards looking with respect to a minimum datetime or backwards looking with respect to a
 * maximum.
 *
 * @author James Brown
 */
public class DurationIntervalDeserializer extends ValueDeserializer<DurationInterval>
{
    /** The name of the first duration within the interval. */
    private final String firstName;

    /** The name of the second duration within the interval. */
    private final String secondName;

    @Override
    public DurationInterval deserialize( JsonParser jp, DeserializationContext context )
    {
        Objects.requireNonNull( jp );

        ObjectMapper mapper = DeclarationFactory.getObjectDeserializer();
        ObjectReadContext reader = jp.objectReadContext();
        JsonNode node = reader.readTree( jp );
        Duration first = DurationDeserializer.getDuration( mapper, node, this.firstName, "unit" );
        Duration second = DurationDeserializer.getDuration( mapper, node, this.secondName, "unit" );

        return new DurationInterval( first, second, node.has( "reverse" )
                                                    && node.get( "reverse" )
                                                           .asBoolean() );
    }

    /**
     * Creates an instance with the specified names for the duration interval quantities.
     * @param firstName the name of the first duration in context
     * @param secondName the name of the second duration in context
     */
    DurationIntervalDeserializer( String firstName, String secondName )
    {
        Objects.requireNonNull( firstName );
        Objects.requireNonNull( secondName );

        this.firstName = firstName;
        this.secondName = secondName;
    }
}

