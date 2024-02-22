package wres.reading.wrds.nwm;

import java.io.IOException;
import java.io.Serial;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import wres.datamodel.MissingValues;

/**
 * Custom deserializer to allow for handling a null value in a NWM data point.
 * @author Hank.Herr
 */
public class NwmDataPointDeserializer extends StdDeserializer<NwmDataPoint>
{
    @Serial
    private static final long serialVersionUID = 5616289115474402095L;

    /**
     * Creates an instance.
     *
     * @param vc the value class
     */
    public NwmDataPointDeserializer( Class<?> vc )
    {
        super( vc );
    }

    /**
     * Creates an instance.
     */
    public NwmDataPointDeserializer()
    {
        this( null );
    }

    @Override
    public NwmDataPoint deserialize( JsonParser jp, DeserializationContext ctxt ) throws IOException
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
