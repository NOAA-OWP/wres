package wres.config.yaml.deserializers;

import java.io.IOException;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.DeclarationFactory;
import wres.config.yaml.components.FormatsBuilder;
import wres.config.yaml.components.Format;
import wres.config.yaml.components.Formats;
import wres.statistics.generated.Outputs;

/**
 * Custom deserializer for a {@link Formats}.
 *
 * @author James Brown
 */
public class FormatsDeserializer extends JsonDeserializer<Formats>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( FormatsDeserializer.class );

    @Override
    public Formats deserialize( JsonParser jp, DeserializationContext context )
            throws IOException
    {
        Objects.requireNonNull( jp );

        ObjectReader mapper = ( ObjectReader ) jp.getCodec();
        JsonNode node = mapper.readTree( jp );

        FormatsBuilder builder = FormatsBuilder.builder();

        int nodeCount = node.size();

        if( nodeCount == 0 )
        {
            LOGGER.debug( "No formats were declared, nothing to deserialize." );
            return null;
        }

        for ( int i = 0; i < nodeCount; i++ )
        {
            JsonNode nextNode = node.get( i );
            this.addFormat( nextNode, builder );
        }

        return builder.build();
    }

    /**
     * Adds a format to the builder.
     * @param node the node to read
     * @param builder the builder to mutate
     */

    private void addFormat( JsonNode node, FormatsBuilder builder )
    {
        // Parameterized format
        if ( node.has( "format" ) )
        {
            this.addParameterizedFormat( node, builder );
        }
        // Simple format
        else
        {
            this.addSimpleFormat( node.asText()
                                      .toLowerCase(),
                                  builder );
        }
    }

    /**
     * Adds a format without parameters to the builder.
     * @param formatName the format name
     * @param builder the builder to mutate
     */
    private void addSimpleFormat( String formatName, FormatsBuilder builder )
    {
        LOGGER.debug( "Encountered a simple format request for {}.", formatName );

        Format format = Format.valueOf( formatName.toUpperCase() );
        switch ( format )
        {
            case PNG -> builder.pngFormat( Formats.PNG_FORMAT );
            case SVG -> builder.svgFormat( Formats.SVG_FORMAT );
            case NETCDF -> builder.netcdfFormat( Formats.NETCDF_FORMAT );
            case NETCDF2 -> builder.netcdf2Format( Formats.NETCDF2_FORMAT );
            case CSV -> builder.csvFormat( Formats.CSV_FORMAT );
            case CSV2 -> builder.csv2Format( Formats.CSV2_FORMAT );
            case PROTOBUF -> builder.protobufFormat( Formats.PROTOBUF_FORMAT );
            case PAIRS -> builder.pairsFormat( Formats.PAIR_FORMAT );
            default -> throw new IllegalArgumentException( "Unrecognized format '" + format + "'." );
        }
    }

    /**
     * Adds a format with parameters to the builder.
     * @param node the node to read
     * @param builder the builder to mutate
     * @throws IllegalArgumentException if the format name is unrecognized
     */
    private void addParameterizedFormat( JsonNode node, FormatsBuilder builder )
    {
        String formatName = node.get( "format" )
                                .asText()
                                .toLowerCase();

        LOGGER.debug( "Encountered a parameterized format request for {}.", formatName );

        Format format = Format.valueOf( formatName.toUpperCase() );
        switch ( format )
        {
            case PNG ->
            {
                Outputs.GraphicFormat.Builder graphicFormatBuilder = Formats.PNG_FORMAT.getOptions()
                                                                               .toBuilder();
                this.addGraphicOptions( graphicFormatBuilder, node );
                Outputs.PngFormat pngFormat = Formats.PNG_FORMAT.toBuilder()
                                                        .setOptions( graphicFormatBuilder )
                                                        .build();
                builder.pngFormat( pngFormat );
            }
            case SVG ->
            {
                Outputs.GraphicFormat.Builder graphicFormatBuilder = Formats.SVG_FORMAT.getOptions()
                                                                               .toBuilder();
                this.addGraphicOptions( graphicFormatBuilder, node );
                Outputs.SvgFormat svgFormat = Formats.SVG_FORMAT.toBuilder()
                                                        .setOptions( graphicFormatBuilder )
                                                        .build();
                builder.svgFormat( svgFormat );
            }
            case NETCDF -> builder.netcdfFormat( Formats.NETCDF_FORMAT );
            case CSV -> builder.csvFormat( Formats.CSV_FORMAT );
            case CSV2 -> builder.csv2Format( Formats.CSV2_FORMAT );
            case PROTOBUF -> builder.protobufFormat( Formats.PROTOBUF_FORMAT );
            default -> throw new IllegalArgumentException( "Unrecognized format '" + format + "'." );
        }
    }

    /**
     * Adds graphic format options, if required.
     * @param graphicFormatBuilder the graphics format builder
     * @param node the node whose graphics format options should be read
     */
    private void addGraphicOptions( Outputs.GraphicFormat.Builder graphicFormatBuilder, JsonNode node )
    {
        if ( node.has( "height" ) )
        {
            JsonNode heightNode = node.get( "height" );
            graphicFormatBuilder.setHeight( heightNode.asInt() );
        }

        if ( node.has( "width" ) )
        {
            JsonNode widthNode = node.get( "width" );
            graphicFormatBuilder.setWidth( widthNode.asInt() );
        }

        if ( node.has( "orientation" ) )
        {
            JsonNode orientationNode = node.get( "orientation" );
            String friendlyText = DeclarationFactory.toEnumName( orientationNode.asText() );
            Outputs.GraphicFormat.GraphicShape shape = Outputs.GraphicFormat.GraphicShape.valueOf( friendlyText );
            graphicFormatBuilder.setShape( shape );
        }
    }
}