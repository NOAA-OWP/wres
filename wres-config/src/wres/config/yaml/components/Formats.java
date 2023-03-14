package wres.config.yaml.components;

import io.soabase.recordbuilder.core.RecordBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import wres.config.yaml.DeclarationFactory;
import wres.statistics.generated.Outputs;

/**
 * @author James Brown
 */
@RecordBuilder
public record Formats( Outputs.PngFormat pngFormat,
                       Outputs.SvgFormat svgFormat,
                       Outputs.CsvFormat csvFormat,
                       Outputs.Csv2Format csv2Format,
                       Outputs.NetcdfFormat netcdfFormat,
                       Outputs.Netcdf2Format netcdf2Format,
                       Outputs.ProtobufFormat protobufFormat,
                       Outputs.PairFormat pairsFormat )
{
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
