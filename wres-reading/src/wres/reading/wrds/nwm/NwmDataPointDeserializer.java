package wres.reading.wrds.nwm;

import java.time.Instant;
import java.time.format.DateTimeFormatter;

import tools.jackson.core.JsonParser;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ValueDeserializer;

import wres.datamodel.MissingValues;
import wres.reading.ReadException;
import wres.reading.ReaderUtilities;

/**
 * Custom deserializer to allow for handling a null value in a NWM data point.
 * @author Hank Herr
 * @author James Brown
 */
public class NwmDataPointDeserializer extends ValueDeserializer<NwmDataPoint>
{

    @Override
    public NwmDataPoint deserialize( JsonParser jp, DeserializationContext ctxt )
    {
        JsonNode node = jp.objectReadContext()
                          .readTree( jp );

        JsonNode timeNode = node.get( "time" );

        String time;

        // Parse the instant.
        if ( timeNode.isString() )
        {
            time = timeNode.asString();
        }
        else
        {
            throw new ReadException( "Could not find a datetime field in the document, which is not allowed." );
        }

        // Lenient formatting in the "basic" ISO8601 format, hours and seconds are optional
        DateTimeFormatter formatter =
                DateTimeFormatter.ofPattern( "[yyyyMMdd'T'HH[:mm[:ss]]'Z'][yyyy-MM-dd'T'HH:mm:ss'Z']" )
                                 .withZone( ReaderUtilities.UTC );

        Instant instant = formatter.parse( time, Instant::from );

        // Parse the value.  Note that if the value is null, the node will not be
        // null.  Rather, isNull will return true.  So there is no need to check
        // explicitly for null.
        double value = MissingValues.DOUBLE;
        if ( !node.get( "value" )
                  .isNull() )
        {
            value = node.get( "value" )
                        .asDouble();
        }

        return new NwmDataPoint( instant, value );
    }
}
