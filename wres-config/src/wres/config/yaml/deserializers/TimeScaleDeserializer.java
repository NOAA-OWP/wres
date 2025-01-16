package wres.config.yaml.deserializers;

import java.io.IOException;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.protobuf.Duration;

import wres.statistics.generated.TimeScale;
import wres.statistics.generated.TimeScale.TimeScaleFunction;

/**
 * Custom deserializer for timescale information.
 *
 * @author James Brown
 */
public class TimeScaleDeserializer extends JsonDeserializer<wres.config.yaml.components.TimeScale>
{
    @Override
    public wres.config.yaml.components.TimeScale deserialize( JsonParser jp, DeserializationContext context )
            throws IOException
    {
        Objects.requireNonNull( jp );

        ObjectReader mapper = ( ObjectReader ) jp.getCodec();
        JsonNode node = mapper.readTree( jp );

        TimeScale.Builder builder = TimeScale.newBuilder()
                                             // Default function
                                             .setFunction( TimeScaleFunction.MEAN );

        if ( node.has( "function" ) )
        {
            JsonNode functionNode = node.get( "function" );
            String functionString = functionNode.asText()
                                                .toUpperCase();
            TimeScaleFunction function = TimeScaleFunction.valueOf( functionString );
            builder.setFunction( function );
        }

        // Unit is dependent required when period is present in the schema: #120552
        if ( node.has( "period" )
             && node.has( "unit" ) )
        {
            java.time.Duration duration = DurationDeserializer.getDuration( mapper, node, "period", "unit" );
            Duration protoDuration = Duration.newBuilder()
                                             .setSeconds( duration.getSeconds() )
                                             .setNanos( duration.getNano() )
                                             .build();
            builder.setPeriod( protoDuration );
        }

        if ( node.has( "minimum_day" ) )
        {
            JsonNode minimumDay = node.get( "minimum_day" );
            int minimumDayInt = minimumDay.asInt();
            builder.setStartDay( minimumDayInt );
        }

        if ( node.has( "maximum_day" ) )
        {
            JsonNode maximumDay = node.get( "maximum_day" );
            int maximumDayInt = maximumDay.asInt();
            builder.setEndDay( maximumDayInt );
        }

        if ( node.has( "minimum_month" ) )
        {
            JsonNode minimumMonth = node.get( "minimum_month" );
            int minimumMonthInt = minimumMonth.asInt();
            builder.setStartMonth( minimumMonthInt );
        }

        if ( node.has( "maximum_month" ) )
        {
            JsonNode maximumMonth = node.get( "maximum_month" );
            int maximumMonthInt = maximumMonth.asInt();
            builder.setEndMonth( maximumMonthInt );
        }

        TimeScale timeScale = builder.build();
        return new wres.config.yaml.components.TimeScale( timeScale );
    }
}

