package wres.config.serializers;

import java.io.IOException;
import java.time.Instant;
import java.util.Objects;
import java.util.Set;

import com.google.protobuf.Timestamp;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.statistics.MessageUtilities;
import wres.statistics.generated.TimeWindow;

/**
 * Serializes a set of {@link TimeWindow}.
 *
 * @author James Brown
 */
public class TimeWindowSerializer extends JsonSerializer<Set<TimeWindow>>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( TimeWindowSerializer.class );

    @Override
    public void serialize( Set<TimeWindow> timeWindows, JsonGenerator gen, SerializerProvider serializers )
            throws IOException
    {
        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Discovered time windows to serialize: {}.", timeWindows );
        }

        gen.writeStartArray();

        for ( TimeWindow timeWindow : timeWindows )
        {
            gen.writeStartObject();

            this.writeTimeWindow( timeWindow, gen );

            gen.writeEndObject();
        }

        gen.writeEndArray();
    }

    @Override
    public boolean isEmpty( SerializerProvider provider, Set<TimeWindow> timeWindows )
    {
        return Objects.isNull( timeWindows ) || timeWindows.isEmpty();
    }

    /**
     * Writes a single time window.
     *
     * @param timeWindow the time window
     * @param writer the writer
     */
    private void writeTimeWindow( TimeWindow timeWindow, JsonGenerator writer ) throws IOException
    {
        if ( timeWindow.hasEarliestLeadDuration()
             || timeWindow.hasLatestLeadDuration() )
        {
            this.writeLeadDurations( timeWindow, writer );
        }

        if ( timeWindow.hasEarliestReferenceTime()
             || timeWindow.hasLatestReferenceTime() )
        {
            this.writeTimestamps( "reference_dates",
                                  timeWindow.getEarliestReferenceTime(),
                                  timeWindow.getLatestReferenceTime(),
                                  writer );
        }

        if ( timeWindow.hasEarliestValidTime()
             || timeWindow.hasLatestValidTime() )
        {
            this.writeTimestamps( "valid_dates",
                                  timeWindow.getEarliestValidTime(),
                                  timeWindow.getLatestValidTime(),
                                  writer );
        }
    }

    /**
     * Writes the lead durations from a time window.
     *
     * @param timeWindow the time window
     * @param writer the writer
     */
    private void writeLeadDurations( TimeWindow timeWindow,
                                     JsonGenerator writer ) throws IOException
    {
        writer.writeFieldName( "lead_times" );

        writer.writeStartObject();

        if ( timeWindow.hasEarliestLeadDuration() )
        {
            writer.writeNumberField( "minimum", timeWindow.getEarliestLeadDuration()
                                                          .getSeconds() );
        }
        if ( timeWindow.hasLatestLeadDuration() )
        {
            writer.writeNumberField( "maximum", timeWindow.getLatestLeadDuration()
                                                          .getSeconds() );
        }

        writer.writeStringField( "unit", "seconds" );

        writer.writeEndObject();
    }

    /**
     * Writes the specified date-time components from a time window.
     *
     * @param name the date-time field name
     * @param earliest the earliest date-time
     * @param latest the latest date-time
     * @param writer the writer
     */
    private void writeTimestamps( String name,
                                  Timestamp earliest,
                                  Timestamp latest,
                                  JsonGenerator writer ) throws IOException
    {
        writer.writeFieldName( name );

        writer.writeStartObject();

        if ( Objects.nonNull( earliest ) )
        {
            Instant instant = MessageUtilities.getInstant( earliest );
            writer.writeStringField( "minimum", instant.toString() );
        }
        if ( Objects.nonNull( latest ) )
        {
            Instant instant = MessageUtilities.getInstant( latest );
            writer.writeStringField( "maximum", instant.toString() );
        }

        writer.writeEndObject();
    }
}
