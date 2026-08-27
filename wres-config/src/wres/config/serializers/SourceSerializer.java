package wres.config.serializers;

import java.util.List;
import java.util.Objects;

import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.SerializationContext;

import wres.config.components.Source;
import wres.config.components.SourceBuilder;
import wres.config.components.UriParameter;

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
            this.writeParametersProperty( source.parameters(), writer );
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

    /**
     * Writes parameters either as a standard dictionary or a multi-dictionary list.
     */
    private void writeParametersProperty( List<UriParameter> parameters, JsonGenerator writer )
    {
        writer.writeName( "parameters" );

        if ( this.hasDuplicateKeys( parameters ) )
        {
            // Multi-dictionary format
            writer.writeStartArray();
            for ( UriParameter param : parameters )
            {
                writer.writeStartObject();
                writer.writeStringProperty( param.key(), param.value() );
                writer.writeEndObject();
            }
            writer.writeEndArray();
        }
        else
        {
            // Standard dictionary
            writer.writeStartObject();
            for ( UriParameter param : parameters )
            {
                writer.writeStringProperty( param.key(), param.value() );
            }
            writer.writeEndObject();
        }
    }

    /**
     * Checks if the list of parameters contains any duplicate keys.
     */
    private boolean hasDuplicateKeys( List<UriParameter> parameters )
    {
        return parameters.stream()
                         .map( UriParameter::key )
                         .distinct()
                         .count() != parameters.size();
    }
}
