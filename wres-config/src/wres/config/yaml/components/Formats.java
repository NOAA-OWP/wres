package wres.config.yaml.components;

import java.util.Objects;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.soabase.recordbuilder.core.RecordBuilder;

import wres.config.yaml.DeclarationFactory;
import wres.config.yaml.deserializers.FormatsDeserializer;
import wres.config.yaml.serializers.FormatsSerializer;
import wres.statistics.generated.Outputs;

/**
 * Wraps a canonical {@link Outputs} and provides a pretty string representation.
 * @author James Brown
 */
@RecordBuilder
@JsonSerialize( using = FormatsSerializer.class )
@JsonDeserialize( using = FormatsDeserializer.class )
public record Formats( Outputs outputs )
{
    /** Defaukt numeric format options. */
    public static Outputs.NumericFormat DEFAULT_NUMERIC_FORMAT = Outputs.NumericFormat.newBuilder()
                                                                                      .build();

    /** Default PNG format. **/
    public static final Outputs.PngFormat PNG_FORMAT =
            Outputs.PngFormat.newBuilder()
                             .setOptions( Outputs.GraphicFormat.newBuilder()
                                                               .setHeight( 600 )
                                                               .setWidth( 800 ) )
                             .build();

    /** Default SVG format. **/
    public static final Outputs.SvgFormat SVG_FORMAT =
            Outputs.SvgFormat.newBuilder()
                             .setOptions( Outputs.GraphicFormat.newBuilder()
                                                               .setHeight( 600 )
                                                               .setWidth( 800 ) )
                             .build();

    /** Default CSV format. **/
    public static final Outputs.CsvFormat CSV_FORMAT = Outputs.CsvFormat.newBuilder()
                                                                        .setOptions( DEFAULT_NUMERIC_FORMAT )
                                                                        .build();

    /** Default CSV2 format. **/
    public static final Outputs.Csv2Format CSV2_FORMAT = Outputs.Csv2Format.newBuilder()
                                                                           .setOptions( DEFAULT_NUMERIC_FORMAT )
                                                                           .build();

    /** Default NetCDF format. **/
    public static final Outputs.NetcdfFormat NETCDF_FORMAT = Outputs.NetcdfFormat.newBuilder()
                                                                                 .setGridded( false )
                                                                                 .setVariableName( "lid" )
                                                                                 .build();

    /** Default NetCDF2 format. **/
    public static final Outputs.Netcdf2Format NETCDF2_FORMAT = Outputs.Netcdf2Format.getDefaultInstance();

    /** Default Protobuf format. **/
    public static final Outputs.ProtobufFormat PROTOBUF_FORMAT = Outputs.ProtobufFormat.getDefaultInstance();

    /** Default pair format. **/
    public static final Outputs.PairFormat PAIR_FORMAT = Outputs.PairFormat.newBuilder()
                                                                           .setOptions( DEFAULT_NUMERIC_FORMAT )
                                                                           .build();

    /**
     * Creates an instance, setting a default format if required.
     * @param outputs the formats
     */

    public Formats
    {
        if ( Objects.isNull( outputs ) )
        {
            outputs = Outputs.newBuilder()
                             .setCsv2( Formats.CSV2_FORMAT )
                             .build();
        }
    }

    @Override
    public String toString()
    {
        // Remove unnecessary whitespace from the JSON protobuf string
        return DeclarationFactory.PROTBUF_STRINGIFIER.apply( this.outputs() );
    }
}
