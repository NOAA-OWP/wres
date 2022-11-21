package wres.io.reading.nwm;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.DateCondition;
import wres.config.generated.NamedFeature;
import wres.config.generated.IntBoundsType;
import wres.config.generated.InterfaceShortHand;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.PairConfig;
import wres.datamodel.time.TimeSeries;
import wres.config.generated.DataSourceConfig.Variable;
import wres.config.generated.DatasourceType;
import wres.io.reading.DataSource;
import wres.io.reading.DataSource.DataDisposition;

/**
 * Tests the {@link NwmVectorReader}.
 * @author James Brown
 */

class NwmVectorReaderTest
{
    /**
     * TODO: implement this test fully, writing a small vector source to an in-memory file system and then reading it.
     * This test will pass currently if using the correct path to the source identified below, which originates from 
     * system test scenario600. For now, this test is ignored.
     */

    @Disabled( "Until we can write a small grid to an in-memory file system, probably using the UCAR NetCDF API." )
    @Test
    void readTwentyFourShortRangeNwmForecastsForOneFeatureProducesTwentyFourTimeSeries()
    {
        // Read a vector source
        Path path = Paths.get( "data/nwmVector/" );

        DataSourceConfig.Source fakeDeclarationSource =
                new DataSourceConfig.Source( path.toUri(),
                                             InterfaceShortHand.NWM_SHORT_RANGE_CHANNEL_RT_CONUS,
                                             null,
                                             null,
                                             null );

        DataSource fakeSource = DataSource.of( DataDisposition.NETCDF_VECTOR,
                                               fakeDeclarationSource,
                                               new DataSourceConfig( DatasourceType.SINGLE_VALUED_FORECASTS,
                                                                     List.of( fakeDeclarationSource ),
                                                                     new Variable( "streamflow", null ),
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
                                                List.of( new NamedFeature( "18384141", "18384141", null ) ),
                                                null,
                                                null,
                                                new IntBoundsType( 0, 18 ),
                                                null,
                                                new DateCondition( "2017-08-07T23:59:59Z", "2017-08-09T17:00:00Z" ),
                                                new DateCondition( "2017-08-07T23:59:59Z", "2017-08-08T23:00:00Z" ),
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null );

        NwmVectorReader reader = NwmVectorReader.of( pairConfig );

        List<TimeSeries<Double>> actual = reader.read( fakeSource )
                                                .map( next -> next.getSingleValuedTimeSeries() )
                                                .collect( Collectors.toList() );

        assertEquals( 24, actual.size() );
    }

}
