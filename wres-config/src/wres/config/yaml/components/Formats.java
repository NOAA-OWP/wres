package wres.config.yaml.components;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.soabase.recordbuilder.core.RecordBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import wres.config.yaml.DeclarationFactory;
import wres.config.yaml.deserializers.FormatsDeserializer;
import wres.config.yaml.serializers.FormatsSerializer;
import wres.statistics.generated.Outputs;

/**
 * @author James Brown
 */
@RecordBuilder
@JsonSerialize( using = FormatsSerializer.class )
@JsonDeserialize( using = FormatsDeserializer.class )
public record Formats( Outputs.PngFormat pngFormat,
                       Outputs.SvgFormat svgFormat,
                       Outputs.CsvFormat csvFormat,
                       Outputs.Csv2Format csv2Format,
                       Outputs.NetcdfFormat netcdfFormat,
                       Outputs.Netcdf2Format netcdf2Format,
                       Outputs.ProtobufFormat protobufFormat,
                       Outputs.PairFormat pairsFormat )
{
    /** A value that is re-used several times. */
    private static final String ZERO = "#0.000000";

    /** Default PNG format. **/
    public static final Outputs.PngFormat PNG_FORMAT =
            Outputs.PngFormat.newBuilder()
                             .setOptions( Outputs.GraphicFormat.newBuilder()
                                                               .setShape( Outputs.GraphicFormat.GraphicShape.LEAD_THRESHOLD )
                                                               .setHeight( 600 )
                                                               .setWidth( 800 ) )
                             .build();

    /** Default SVG format. **/
    public static final Outputs.SvgFormat SVG_FORMAT =
            Outputs.SvgFormat.newBuilder()
                             .setOptions( Outputs.GraphicFormat.newBuilder()
                                                               .setShape( Outputs.GraphicFormat.GraphicShape.LEAD_THRESHOLD ) )
                             .build();

    /** Default CSV format. **/
    public static final Outputs.CsvFormat CSV_FORMAT =
            Outputs.CsvFormat.newBuilder()
                             .setOptions( Outputs.NumericFormat.newBuilder()
                                                               .setDecimalFormat( ZERO ) )
                             .build();

    /** Default CSV2 format. **/
    public static final Outputs.Csv2Format CSV2_FORMAT =
            Outputs.Csv2Format.newBuilder()
                              .setOptions( Outputs.NumericFormat.newBuilder()
                                                                .setDecimalFormat( ZERO ) )
                              .build();

    /** Default NetCDF format. **/
    public static final Outputs.NetcdfFormat NETCDF_FORMAT =
            Outputs.NetcdfFormat.getDefaultInstance();

    /** Default NetCDF2 format. **/
    public static final Outputs.Netcdf2Format NETCDF2_FORMAT =
            Outputs.Netcdf2Format.getDefaultInstance();

    /** Default Protobuf format. **/
    public static final Outputs.ProtobufFormat PROTOBUF_FORMAT =
            Outputs.ProtobufFormat.getDefaultInstance();

    /** Default pair format. **/
    public static final Outputs.PairFormat PAIR_FORMAT =
            Outputs.PairFormat.newBuilder()
                              .setOptions( Outputs.NumericFormat.newBuilder()
                                                                .setDecimalFormat( ZERO ) )
                              .build();

    @Override
    public String toString()
    {
        // Remove unnecessary whitespace from the JSON protobuf string
        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE )
                .append( "pngFormat", DeclarationFactory.PROTBUF_STRINGIFIER.apply( pngFormat ) )
                .append( "svgFormat", DeclarationFactory.PROTBUF_STRINGIFIER.apply( svgFormat ) )
                .append( "csvFormat", DeclarationFactory.PROTBUF_STRINGIFIER.apply( csvFormat ) )
                .append( "csv2Format", DeclarationFactory.PROTBUF_STRINGIFIER.apply( csv2Format ) )
                .append( "netcdfFormat", DeclarationFactory.PROTBUF_STRINGIFIER.apply( netcdfFormat ) )
                .append( "netcdf2Format", DeclarationFactory.PROTBUF_STRINGIFIER.apply( netcdf2Format ) )
                .append( "protobufFormat", DeclarationFactory.PROTBUF_STRINGIFIER.apply( protobufFormat ) )
                .append( "pairsFormat", DeclarationFactory.PROTBUF_STRINGIFIER.apply( pairsFormat ) )
                .build();
    }
}
