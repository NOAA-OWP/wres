package wres.config.deserializers;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.protobuf.Timestamp;

import wres.config.components.LeadTimeInterval;
import wres.config.components.TimeInterval;
import wres.statistics.MessageUtilities;
import wres.statistics.generated.TimeWindow;

/**
 * Custom deserializer for a {@link TimeWindow}.
 *
 * @author James Brown
 */
public class TimeWindowDeserializer extends JsonDeserializer<Set<TimeWindow>>
{
    @Override
    public Set<TimeWindow> deserialize( JsonParser jp, DeserializationContext context )
            throws IOException
    {
        Set<TimeWindow> timeWindows = new HashSet<>();

        ObjectReader mapper = ( ObjectReader ) jp.getCodec();
        JsonNode node = mapper.readTree( jp );

        if ( node.isArray() )
        {
            int count = node.size();
            for ( int i = 0; i < count; i++ )
            {
                TimeWindow.Builder builder = TimeWindow.newBuilder();

                JsonNode nextNode = node.get( i );

                // Valid dates
                if ( nextNode.has( "valid_dates" ) )
                {
                    JsonNode validDatesNode = nextNode.get( "valid_dates" );
                    TimeInterval validDates = mapper.readValue( validDatesNode, TimeInterval.class );

                    Timestamp minimum = MessageUtilities.getTimestamp( validDates.minimum() );
                    Timestamp maximum = MessageUtilities.getTimestamp( validDates.maximum() );
                    builder.setEarliestValidTime( minimum )
                           .setLatestValidTime( maximum );
                }

                // Reference dates
                if ( nextNode.has( "reference_dates" ) )
                {
                    JsonNode referenceDatesNode = nextNode.get( "reference_dates" );
                    TimeInterval referenceDates = mapper.readValue( referenceDatesNode, TimeInterval.class );

                    Timestamp minimum = MessageUtilities.getTimestamp( referenceDates.minimum() );
                    Timestamp maximum = MessageUtilities.getTimestamp( referenceDates.maximum() );
                    builder.setEarliestReferenceTime( minimum )
                           .setLatestReferenceTime( maximum );
                }

                // Lead times?
                if ( nextNode.has( "lead_times" ) )
                {
                    JsonNode leadTimesNode = nextNode.get( "lead_times" );
                    LeadTimeInterval leadTimes = mapper.readValue( leadTimesNode, LeadTimeInterval.class );

                    com.google.protobuf.Duration minimum = MessageUtilities.getDuration( leadTimes.minimum() );
                    com.google.protobuf.Duration maximum = MessageUtilities.getDuration( leadTimes.maximum() );
                    builder.setEarliestLeadDuration( minimum )
                           .setLatestLeadDuration( maximum );
                }

                TimeWindow nextWindow = builder.build();
                timeWindows.add( nextWindow );
            }
        }

        return Collections.unmodifiableSet( timeWindows );
    }
}