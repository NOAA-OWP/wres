package wres.config.yaml.deserializers;

import java.io.IOException;
import java.time.Month;
import java.time.MonthDay;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;

import wres.config.yaml.components.Season;

/**
 * Custom deserializer for a {@link Season}.
 *
 * @author James Brown
 */
public class SeasonDeserializer extends JsonDeserializer<Season>
{
    @Override
    public Season deserialize( JsonParser jp, DeserializationContext context )
            throws IOException
    {
        Objects.requireNonNull( jp );

        ObjectReader mapper = ( ObjectReader ) jp.getCodec();
        JsonNode node = mapper.readTree( jp );

        int minimumDay = 1;
        int minimumMonth = 1;
        int maximumDay;  // Either declared or determined from the maximum month
        int maximumMonth = 12;

        if ( node.has( "minimum_month" ) )
        {
            JsonNode minimumMonthNode = node.get( "minimum_month" );
            minimumMonth = minimumMonthNode.asInt();
        }

        if ( node.has( "maximum_month" ) )
        {
            JsonNode maximumMonthNode = node.get( "maximum_month" );
            maximumMonth = maximumMonthNode.asInt();
        }

        if ( node.has( "minimum_day" ) )
        {
            JsonNode minimumDayNode = node.get( "minimum_day" );
            minimumDay = minimumDayNode.asInt();
        }

        if ( node.has( "maximum_day" ) )
        {
            JsonNode maximumDayNode = node.get( "maximum_day" );
            maximumDay = maximumDayNode.asInt();
        }
        else
        {
            Month month = Month.of( maximumMonth );
            maximumDay = month.maxLength();
        }

        MonthDay minimum = MonthDay.of( minimumMonth, minimumDay );
        MonthDay maximum = MonthDay.of( maximumMonth, maximumDay );

        return new Season( minimum, maximum );
    }
}

