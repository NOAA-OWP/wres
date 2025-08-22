package wres.config.deserializers;

import java.io.IOException;
import java.time.Duration;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;

/**
 * Custom deserializer for a pair of {@link Duration} that represents an interval, together with an indicator about
 * whether the interval is forwards looking with respect to a minimum datetime or backwards looking with respect to a
 * maximum.
 *
 * @author James Brown
 */
public class DurationIntervalDeserializer extends JsonDeserializer<DurationInterval>
{
    /** The name of the first duration within the interval. */
    private final String firstName;

    /** The name of the second duration within the interval. */
    private final String secondName;

    @Override
    public DurationInterval deserialize( JsonParser jp, DeserializationContext context )
            throws IOException
    {
        Objects.requireNonNull( jp );

        ObjectReader mapper = ( ObjectReader ) jp.getCodec();
        JsonNode node = mapper.readTree( jp );
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

