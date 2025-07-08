package wres.statistics;

import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import wres.statistics.generated.Covariate;
import wres.statistics.generated.Outputs;
import wres.statistics.generated.Consumer.Format;
import wres.statistics.generated.Outputs.Csv2Format;
import wres.statistics.generated.Outputs.CsvFormat;
import wres.statistics.generated.Outputs.Netcdf2Format;
import wres.statistics.generated.Outputs.PngFormat;
import wres.statistics.generated.Outputs.ProtobufFormat;
import wres.statistics.generated.Outputs.SvgFormat;

/**
 * Tests the {@link MessageUtilities}.
 *
 * @author James Brown
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
                                 .setNetcdf2( Netcdf2Format.getDefaultInstance() )
                                 .setProtobuf( ProtobufFormat.getDefaultInstance() )
                                 .build();

        Set<Format> actual = MessageUtilities.getDeclaredFormats( outputs );

        assertEquals( expected, actual );
    }

    @Test
    void testToStringForCovariateWithMinimumAndMaximum()
    {
        Covariate covariate = Covariate.newBuilder()
                                       .setMaximumInclusiveValue( 4.3 )
                                       .setMinimumInclusiveValue( 2.1 )
                                       .setVariableName( "foo" )
                                       .build();

        assertEquals( "2.1 <= foo <= 4.3", MessageUtilities.toString( covariate ) );
    }

    @Test
    void testToStringForCovariateWithMinimumOnly()
    {
        Covariate covariate = Covariate.newBuilder()
                                       .setMinimumInclusiveValue( 2.1 )
                                       .setVariableName( "foo" )
                                       .build();

        assertEquals( "foo >= 2.1", MessageUtilities.toString( covariate ) );
    }

    @Test
    void testToStringForCovariateWithMaximumOnly()
    {
        Covariate covariate = Covariate.newBuilder()
                                       .setMaximumInclusiveValue( 4.3 )
                                       .setVariableName( "foo" )
                                       .build();

        assertEquals( "foo <= 4.3", MessageUtilities.toString( covariate ) );
    }
}
