package wres.statistics;

import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import wres.statistics.generated.Outputs;
import wres.statistics.generated.Consumer.Format;
import wres.statistics.generated.Outputs.Csv2Format;
import wres.statistics.generated.Outputs.CsvFormat;
import wres.statistics.generated.Outputs.NetcdfFormat;
import wres.statistics.generated.Outputs.PngFormat;
import wres.statistics.generated.Outputs.ProtobufFormat;
import wres.statistics.generated.Outputs.SvgFormat;

/**
 * Tests the {@link MessageUtilities}.
 * 
 * @author james.brown@hydrosolved.com
 */

class MessageUtilitiesTest
{

    @Test
    void testDeclaredFormats()
    {
        Set<Format> expected = new HashSet<>( Set.of( Format.values() ) );

        // Formats that are unsupported by Outputs
        expected.remove( Format.UNRECOGNIZED );
        expected.remove( Format.PAIRS );

        Outputs outputs = Outputs.newBuilder()
                                 .setCsv( CsvFormat.getDefaultInstance() )
                                 .setPng( PngFormat.getDefaultInstance() )
                                 .setSvg( SvgFormat.newBuilder() )
                                 .setCsv2( Csv2Format.getDefaultInstance() )
                                 .setProtobuf( ProtobufFormat.getDefaultInstance() )
                                 .setNetcdf( NetcdfFormat.newBuilder() )
                                 .build();

        Set<Format> actual = MessageUtilities.getDeclaredFormats( outputs );

        assertEquals( expected, actual );
    }
}
