package wres.config.yaml;

import java.io.IOException;
import java.time.temporal.ChronoUnit;
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
 * Custom deserializer for time scale information.
 * 
 * @author James Brown
 */
class TimeScaleDeserializer extends JsonDeserializer<TimeScale>
{
    @Override
    public TimeScale deserialize( JsonParser jp, DeserializationContext context )
            throws IOException
    {
        Objects.requireNonNull( jp );

        ObjectReader mapper = (ObjectReader) jp.getCodec();
        JsonNode node = mapper.readTree( jp );

        TimeScale.Builder builder = TimeScale.newBuilder();

        if ( node.has( "function" ) )
        {
            JsonNode functionNode = node.get( "function" );
            String functionString = functionNode.asText()
                                                .toUpperCase();
            TimeScaleFunction function = TimeScaleFunction.valueOf( functionString );
            builder.setFunction( function );
        }

        if ( node.has( "period" ) && node.has( "unit" ) )
        {
            JsonNode periodNode = node.get( "period" );
            JsonNode unitNode = node.get( "unit" );
            String unitString = unitNode.asText();
            ChronoUnit chronoUnit = mapper.readValue( unitString, ChronoUnit.class );
            int period = periodNode.asInt();
            java.time.Duration duration = java.time.Duration.of( period, chronoUnit );
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

        return builder.build();
    }
}

