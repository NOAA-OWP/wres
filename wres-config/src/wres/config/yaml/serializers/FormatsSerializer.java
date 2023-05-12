package wres.config.yaml.serializers;

import java.io.IOException;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.DeclarationUtilities;
import wres.config.yaml.components.Formats;
import wres.config.yaml.components.FormatsBuilder;
import wres.statistics.generated.Outputs;

/**
 * Serializes a {@link Formats}.
 * @author James Brown
 */
public class FormatsSerializer extends JsonSerializer<Formats>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( FormatsSerializer.class );

    @Override
    public void serialize( Formats formats, JsonGenerator writer, SerializerProvider serializers ) throws IOException
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
        if ( outputs.hasCsv() )
        {
            writer.writeString( "csv" );
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
    public boolean isEmpty( SerializerProvider serializers, Formats formats )
    {
        return Objects.isNull( formats ) || formats.equals( FormatsBuilder.builder()
                                                                          .build() );
    }

    /**
     * Writes the formats that support parameter values,
     * @param outputs the outputs
     * @param writer the writer
     * @throws IOException if the writing failed for any reason
     */

    private void writeParameterizedFormats( Outputs outputs,
                                            JsonGenerator writer ) throws IOException
    {
        // Write the formats that allow parameters
        if ( outputs.hasNetcdf() )
        {
            // Non-default parameter values
            if ( !Formats.NETCDF_FORMAT.equals( outputs.getNetcdf() ) )
            {
                this.writeNonDefaultNetcdfFormat( outputs.getNetcdf(),
                                                  writer );
            }
            // All default parameter values, do not write them
            else
            {
                writer.writeString( "netcdf" );
            }
        }
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
     * @throws IOException if the format could not be written for any reason
     */
    private void writeNonDefaultGraphicsFormat( String formatName,
                                                Outputs.GraphicFormat parameters,
                                                Outputs.GraphicFormat defaults,
                                                JsonGenerator writer ) throws IOException
    {
        LOGGER.debug( "Discovered a graphic format with non-default parameter values." );

        // Start
        writer.writeStartObject();

        writer.writeStringField( "format", formatName );

        if ( parameters.getWidth() != defaults.getWidth() )
        {
            writer.writeNumberField( "width", parameters.getWidth() );
        }

        if ( parameters.getHeight() != defaults.getHeight() )
        {
            writer.writeNumberField( "height", parameters.getHeight() );
        }

        if ( parameters.getShape() != defaults.getShape() )
        {
            Outputs.GraphicFormat.GraphicShape shape = parameters.getShape();
            String friendlyShape = DeclarationUtilities.fromEnumName( shape.name() );
            writer.writeStringField( "orientation", friendlyShape );
        }

        // End
        writer.writeEndObject();
    }

    /**
     * Writes a graphic format with associated (non-default) parameters.
     * @param parameters the format parameters
     * @throws IOException if the format could not be written for any reason
     */
    private void writeNonDefaultNetcdfFormat( Outputs.NetcdfFormat parameters,
                                              JsonGenerator writer ) throws IOException
    {
        LOGGER.debug( "Discovered a NetCDF format with non-default parameter values." );

        // Start
        writer.writeStartObject();

        Outputs.NetcdfFormat defaults = Formats.NETCDF_FORMAT;

        writer.writeStringField( "format", "netcdf" );

        if ( ! parameters.getTemplatePath().isBlank()
             && !Objects.equals( parameters.getTemplatePath(), defaults.getTemplatePath() ) )
        {
            writer.writeStringField( "template_path", parameters.getTemplatePath() );
        }

        if ( ! parameters.getVariableName().isBlank()
             && !Objects.equals( parameters.getVariableName(), defaults.getVariableName() ) )
        {
            writer.writeStringField( "variable_name", parameters.getVariableName() );
        }

        if ( parameters.getGridded() )
        {
            writer.writeBooleanField( "gridded", true );
        }

        // End
        writer.writeEndObject();
    }
}
