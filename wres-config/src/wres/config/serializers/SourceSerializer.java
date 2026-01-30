package wres.config.serializers;

import java.util.Objects;

import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.SerializationContext;

import wres.config.components.Source;
import wres.config.components.SourceBuilder;

/**
 * Serializes a {@link Source}.
 * @author James Brown
 */
public class SourceSerializer extends ValueSerializer<Source>
{
    @Override
    public void serialize( Source source, JsonGenerator writer, SerializationContext serializers )
    {
        // Skip the URI if that is the only attribute present
        Source compare = SourceBuilder.builder()
                                      .uri( source.uri() )
                                      .build();
        // URI only
        if ( compare.equals( source )
             && Objects.nonNull( source.uri() ) )
        {
            writer.writeString( source.uri()
                                      .toString() );
        }
        // Full description
        else
        {
            this.writeSource( source, writer );
        }
    }

    /**
     * Writes a full data source description.
     * @param source the source
     * @param writer the writer
     */

    private void writeSource( Source source, JsonGenerator writer )
    {
        writer.writeStartObject();

        if ( Objects.nonNull( source.uri() ) )
        {
            writer.writePOJOProperty( "uri", source.uri() );
        }

        if ( Objects.nonNull( source.sourceInterface() ) )
        {
            writer.writePOJOProperty( "interface", source.sourceInterface() );
        }

        if ( Objects.nonNull( source.parameters() )
             && !source.parameters().isEmpty() )
        {
            writer.writePOJOProperty( "parameters", source.parameters() );
        }

        if ( Objects.nonNull( source.pattern() ) )
        {
            writer.writeStringProperty( "pattern", source.pattern() );
        }

        if ( Objects.nonNull( source.timeZoneOffset() ) )
        {
            writer.writeStringProperty( "time_zone_offset", source.timeZoneOffset()
                                                                  .toString()
                                                                  .replace( ":", "" ) );
        }

        if ( Objects.nonNull( source.daylightSavings() ) )
        {
            writer.writeBooleanProperty( "daylight_savings", source.daylightSavings() );
        }

        if ( Objects.nonNull( source.missingValue() )
             && !source.missingValue()
                       .isEmpty() )
        {
            writer.writePOJOProperty( "missing_value", source.missingValue() );
        }

        writer.writeEndObject();
    }
}
