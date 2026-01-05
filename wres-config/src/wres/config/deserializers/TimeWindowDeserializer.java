package wres.config.deserializers;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import tools.jackson.core.JsonParser;
import tools.jackson.core.ObjectReadContext;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectReader;
import com.google.protobuf.Timestamp;

import wres.config.DeclarationFactory;
import wres.config.components.LeadTimeInterval;
import wres.config.components.TimeInterval;
import wres.statistics.MessageUtilities;
import wres.statistics.generated.TimeWindow;

/**
 * Custom deserializer for a {@link TimeWindow}.
 *
 * @author James Brown
 */
public class TimeWindowDeserializer extends ValueDeserializer<Set<TimeWindow>>
{
    @Override
    public Set<TimeWindow> deserialize( JsonParser jp, DeserializationContext context )
    {
        Set<TimeWindow> timeWindows = new HashSet<>();

        ObjectReadContext reader = jp.objectReadContext();
        ObjectMapper mapper = DeclarationFactory.getObjectDeserializer();
        ObjectReader timeIntervalReader = mapper.readerFor( TimeInterval.class );
        ObjectReader leadIntervalReader = mapper.readerFor( LeadTimeInterval.class );

        JsonNode node = reader.readTree( jp );

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
                    TimeInterval validDates = timeIntervalReader.readValue( validDatesNode );

                    Timestamp minimum = MessageUtilities.getTimestamp( validDates.minimum() );
                    Timestamp maximum = MessageUtilities.getTimestamp( validDates.maximum() );
                    builder.setEarliestValidTime( minimum )
                           .setLatestValidTime( maximum );
                }

                // Reference dates
                if ( nextNode.has( "reference_dates" ) )
                {
                    JsonNode referenceDatesNode = nextNode.get( "reference_dates" );
                    TimeInterval referenceDates = timeIntervalReader.readValue( referenceDatesNode );

                    Timestamp minimum = MessageUtilities.getTimestamp( referenceDates.minimum() );
                    Timestamp maximum = MessageUtilities.getTimestamp( referenceDates.maximum() );
                    builder.setEarliestReferenceTime( minimum )
                           .setLatestReferenceTime( maximum );
                }

                // Lead times?
                if ( nextNode.has( "lead_times" ) )
                {
                    JsonNode leadTimesNode = nextNode.get( "lead_times" );
                    LeadTimeInterval leadTimes = leadIntervalReader.readValue( leadTimesNode );

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