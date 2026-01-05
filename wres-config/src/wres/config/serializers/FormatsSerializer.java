package wres.config.serializers;

import java.util.Objects;

import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.SerializationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.DeclarationUtilities;
import wres.config.components.Formats;
import wres.config.components.FormatsBuilder;
import wres.statistics.generated.Outputs;

/**
 * Serializes a {@link Formats}.
 * @author James Brown
 */
public class FormatsSerializer extends ValueSerializer<Formats>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( FormatsSerializer.class );

    @Override
    public void serialize( Formats formats, JsonGenerator writer, SerializationContext serializers )
    {
        writer.writeStartArray();

        Outputs outputs = formats.outputs();

        // Write the formats without parameters
        if ( outputs.hasProtobuf() )
        {
            writer.writeString( "protobuf" );
        }
        if ( outputs.hasNetcdf2() )
        {
            writer.writeString( "netcdf2" );
        }

        // The numeric formats do not have effective parameters because the decimal precision is abstracted away, one
        // for all formats currently
        if ( outputs.hasCsv2() )
        {
            writer.writeString( "csv2" );
        }
        if ( outputs.hasPairs() )
        {
            writer.writeString( "pairs" );
        }

        // Write the parameterized formats
        this.writeParameterizedFormats( outputs, writer );

        writer.writeEndArray();
    }

    @Override
    public boolean isEmpty( SerializationContext serializers, Formats formats )
    {
        return Objects.isNull( formats ) || formats.equals( FormatsBuilder.builder()
                                                                          .build() );
    }

    /**
     * Writes the formats that support parameter values,
     * @param outputs the outputs
     * @param writer the writer
     */

    private void writeParameterizedFormats( Outputs outputs,
                                            JsonGenerator writer )
    {
        if ( outputs.hasPng() )
        {
            // Non-default parameter values
            if ( !Formats.PNG_FORMAT.equals( outputs.getPng() ) )
            {
                this.writeNonDefaultGraphicsFormat( "png",
                                                    outputs.getPng()
                                                           .getOptions(),
                                                    Formats.PNG_FORMAT.getOptions(),
                                                    writer );
            }
            // All default parameter values, do not write them
            else
            {
                writer.writeString( "png" );
            }
        }
        if ( outputs.hasSvg() )
        {
            // Non-default parameter values
            if ( !Formats.SVG_FORMAT.equals( outputs.getSvg() ) )
            {
                this.writeNonDefaultGraphicsFormat( "svg",
                                                    outputs.getSvg()
                                                           .getOptions(),
                                                    Formats.SVG_FORMAT.getOptions(),
                                                    writer );
            }
            // All default parameter values, do not write them
            else
            {
                writer.writeString( "svg" );
            }
        }
    }

    /**
     * Writes a graphic format with associated (non-default) parameters.
     * @param formatName the format name
     * @param parameters the format parameters
     * @param defaults the default parameter values, which should not be written
     */
    private void writeNonDefaultGraphicsFormat( String formatName,
                                                Outputs.GraphicFormat parameters,
                                                Outputs.GraphicFormat defaults,
                                                JsonGenerator writer )
    {
        LOGGER.debug( "Discovered a graphic format with non-default parameter values." );

        // Start
        writer.writeStartObject();

        writer.writeStringProperty( "format", formatName );

        if ( parameters.getWidth() != defaults.getWidth() )
        {
            writer.writeNumberProperty( "width", parameters.getWidth() );
        }

        if ( parameters.getHeight() != defaults.getHeight() )
        {
            writer.writeNumberProperty( "height", parameters.getHeight() );
        }

        if ( parameters.getShape() != defaults.getShape() )
        {
            Outputs.GraphicFormat.GraphicShape shape = parameters.getShape();
            String friendlyShape = DeclarationUtilities.fromEnumName( shape.name() );
            writer.writeStringProperty( "orientation", friendlyShape );
        }

        // End
        writer.writeEndObject();
    }
}
