package wres.config.deserializers;

import java.util.Objects;

import tools.jackson.core.JsonParser;
import tools.jackson.core.ObjectReadContext;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.JsonNode;
import com.google.protobuf.Duration;

import wres.config.DeclarationFactory;
import wres.statistics.generated.TimeScale;
import wres.statistics.generated.TimeScale.TimeScaleFunction;

/**
 * Custom deserializer for timescale information.
 *
 * @author James Brown
 */
public class TimeScaleDeserializer extends ValueDeserializer<wres.config.components.TimeScale>
{
    @Override
    public wres.config.components.TimeScale deserialize( JsonParser jp, DeserializationContext context )
    {
        Objects.requireNonNull( jp );

        ObjectReadContext reader = jp.objectReadContext();
        ObjectMapper mapper = DeclarationFactory.getObjectDeserializer();
        JsonNode node = reader.readTree( jp );

        TimeScale.Builder builder = TimeScale.newBuilder()
                                             // Default function
                                             .setFunction( TimeScaleFunction.MEAN );

        if ( node.has( "function" ) )
        {
            JsonNode functionNode = node.get( "function" );
            String functionString = functionNode.asString()
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
        return new wres.config.components.TimeScale( timeScale );
    }
}

