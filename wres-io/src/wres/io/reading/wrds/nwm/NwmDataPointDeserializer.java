package wres.io.reading.wrds.nwm;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import wres.datamodel.MissingValues;

/**
 * Custom deserializer to allow for handling a null value in a NWM data point.
 * @author Hank.Herr
 *
 */
public class NwmDataPointDeserializer extends StdDeserializer<NwmDataPoint>
{
    private static final long serialVersionUID = 5616289115474402095L;

    public NwmDataPointDeserializer()
    {
        this( null );
    }

    public NwmDataPointDeserializer( Class<?> vc )
    {
        super( vc );
    }

    @Override
    public NwmDataPoint deserialize( JsonParser jp, DeserializationContext ctxt )
            throws IOException, JsonProcessingException
    {
        JsonNode node = jp.getCodec().readTree( jp );

        //Parse the instant.
        DateTimeFormatter formatter = new DateTimeFormatterBuilder()
                                                                    .appendPattern( "uuuuMMdd'T'HHX" )
                                                                    .toFormatter();
        Instant instant = formatter.parse( node.get( "time" ).asText(), Instant::from );

        //Parse the value.  Note that if the value is null, the node will not be
        //null.  Rather, isNull will return true.  So there is not need to check
        //explicitly for null.
        double value = MissingValues.DOUBLE;
        if ( !node.get( "value" ).isNull() )
        {
            value = Double.parseDouble( node.get( "value" ).asText() );
        }

        return new NwmDataPoint( instant, value );
    }
}
