package wres.io.reading.nwm;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigInteger;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import wres.config.generated.Circle;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.DateCondition;
import wres.config.generated.IntBoundsType;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.PairConfig;
import wres.config.generated.UnnamedFeature;
import wres.datamodel.time.TimeSeries;
import wres.config.generated.DataSourceConfig.Variable;
import wres.io.reading.DataSource;
import wres.io.reading.DataSource.DataDisposition;

/**
 * Tests the {@link NwmGriddedReader}.
 * @author James Brown
 */

class NwmGriddedReaderTest
{
    /**
     * TODO: implement this test fully, writing a small gridded source to an in-memory file system and then reading it.
     * This test will pass currently if using the correct path to the source identified below, which originates from 
     * system test scenario650. As of 20220824, the gridded cache must be constructed and supplied, so it will not work
     * until this is done. For now, this test is ignored.
     */

    @Disabled( "Until we can write a small grid to an in-memory file system, probably using the UCAR NetCDF API." )
    @Test
    void readOneNwmGridProducesEighteenTimeSeries()
    {
        // Read a gridded source
        Path path = Paths.get( "nwm.20180526", "nwm.t2018052600z.short_range.forcing.f005.wres.nc" );

        DataSourceConfig.Source fakeDeclarationSource =
                new DataSourceConfig.Source( path.toUri(),
                                             null,
                                             null,
                                             null,
                                             null );

        DataSource fakeSource = DataSource.of( DataDisposition.NETCDF_GRIDDED,
                                               fakeDeclarationSource,
                                               new DataSourceConfig( null,
                                                                     List.of( fakeDeclarationSource ),
                                                                     new Variable( "RAINRATE", null ),
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null ),
                                               Collections.emptyList(),
                                               // Use a fake URI with an NWIS-like string as this is used to trigger the 
                                               // identification of an instantaneous time-scale 
                                               path.toUri(),
                                               LeftOrRightOrBaseline.RIGHT );

        PairConfig pairConfig = new PairConfig( null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                List.of( new UnnamedFeature( null,
                                                                             null,
                                                                             new Circle( 39.225F,
                                                                                         -76.825F,
                                                                                         0.05F,
                                                                                         BigInteger.valueOf( 2346 ) ) ) ),
                                                new IntBoundsType( 1, 18 ),
                                                null,
                                                new DateCondition( "2018-01-01T00:00:00Z", "2021-01-01T00:00:00Z" ),
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null );

        // TODO: create the gridded features cache here, rather than supplying null
        NwmGriddedReader reader = NwmGriddedReader.of( pairConfig, null );

        List<TimeSeries<Double>> actual = reader.read( fakeSource )
                                                .map( next -> next.getSingleValuedTimeSeries() )
                                                .collect( Collectors.toList() );

        assertEquals( 18, actual.size() );
    }

}
