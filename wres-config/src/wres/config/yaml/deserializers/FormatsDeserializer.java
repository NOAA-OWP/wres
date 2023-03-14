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

    /** Default PNG format. **/
    private static final Outputs.PngFormat PNG_FORMAT =
            Outputs.PngFormat.newBuilder()
                             .setOptions( Outputs.GraphicFormat.newBuilder()
                                                               .setShape( Outputs.GraphicFormat.GraphicShape.LEAD_THRESHOLD )
                                                               .setHeight( 600 )
                                                               .setWidth( 800 ) )
                             .build();

    /** Default SVG format. **/
    private static final Outputs.SvgFormat SVG_FORMAT =
            Outputs.SvgFormat.newBuilder()
                             .setOptions( Outputs.GraphicFormat.newBuilder()
                                                               .setShape( Outputs.GraphicFormat.GraphicShape.LEAD_THRESHOLD ) )
                             .build();

    /** Default CSV format. **/
    private static final Outputs.CsvFormat CSV_FORMAT =
            Outputs.CsvFormat.newBuilder()
                             .setOptions( Outputs.NumericFormat.newBuilder()
                                                               .setDecimalFormat( "0.000000" ) )
                             .build();

    /** Default CSV2 format. **/
    private static final Outputs.Csv2Format CSV2_FORMAT =
            Outputs.Csv2Format.newBuilder()
                              .setOptions( Outputs.NumericFormat.newBuilder()
                                                                .setDecimalFormat( "0.000000" ) )
                              .build();

    /** Default NetCDF format. **/
    private static final Outputs.NetcdfFormat NETCDF_FORMAT =
            Outputs.NetcdfFormat.getDefaultInstance();

    /** Default NetCDF2 format. **/
    private static final Outputs.Netcdf2Format NETCDF2_FORMAT =
            Outputs.Netcdf2Format.getDefaultInstance();

    /** Default Protobuf format. **/
    private static final Outputs.ProtobufFormat PROTOBUF_FORMAT =
            Outputs.ProtobufFormat.getDefaultInstance();

    /** Default pair format. **/
    private static final Outputs.PairFormat PAIR_FORMAT =
            Outputs.PairFormat.newBuilder()
                              .setOptions( Outputs.NumericFormat.newBuilder()
                                                                .setDecimalFormat( "0.000000" ) )
                              .build();

    @Override
    public Formats deserialize( JsonParser jp, DeserializationContext context )
            throws IOException
    {
        Objects.requireNonNull( jp );

        ObjectReader mapper = ( ObjectReader ) jp.getCodec();
        JsonNode node = mapper.readTree( jp );

        FormatsBuilder builder = FormatsBuilder.builder();

        int nodeCount = node.size();

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
        // Paramaterized format
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
            case PNG -> builder.pngFormat( PNG_FORMAT );
            case SVG -> builder.svgFormat( SVG_FORMAT );
            case NETCDF -> builder.netcdfFormat( NETCDF_FORMAT );
            case NETCDF2 -> builder.netcdf2Format( NETCDF2_FORMAT );
            case CSV -> builder.csvFormat( CSV_FORMAT );
            case CSV2 -> builder.csv2Format( CSV2_FORMAT );
            case PROTOBUF -> builder.protobufFormat( PROTOBUF_FORMAT );
            case PAIRS -> builder.pairsFormat( PAIR_FORMAT );
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
                Outputs.GraphicFormat.Builder graphicFormatBuilder = PNG_FORMAT.getOptions()
                                                                               .toBuilder();
                this.addGraphicOptions( graphicFormatBuilder, node );
                Outputs.PngFormat pngFormat = PNG_FORMAT.toBuilder()
                                                        .setOptions( graphicFormatBuilder )
                                                        .build();
                builder.pngFormat( pngFormat );
            }
            case SVG ->
            {
                Outputs.GraphicFormat.Builder graphicFormatBuilder = SVG_FORMAT.getOptions()
                                                                               .toBuilder();
                this.addGraphicOptions( graphicFormatBuilder, node );
                Outputs.SvgFormat svgFormat = SVG_FORMAT.toBuilder()
                                                        .setOptions( graphicFormatBuilder )
                                                        .build();
                builder.svgFormat( svgFormat );
            }
            case NETCDF -> builder.netcdfFormat( NETCDF_FORMAT );
            case CSV -> builder.csvFormat( CSV_FORMAT );
            case CSV2 -> builder.csv2Format( CSV2_FORMAT );
            case PROTOBUF -> builder.protobufFormat( PROTOBUF_FORMAT );
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
            String friendlyText = DeclarationFactory.getEnumFriendlyName( orientationNode );
            Outputs.GraphicFormat.GraphicShape shape = Outputs.GraphicFormat.GraphicShape.valueOf( friendlyText );
            graphicFormatBuilder.setShape( shape );
        }
    }
}