package wres.reading.netcdf.nwm;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import wres.config.yaml.components.DataType;
import wres.config.yaml.components.DatasetBuilder;
import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.EvaluationDeclarationBuilder;
import wres.config.yaml.components.Features;
import wres.config.yaml.components.FeaturesBuilder;
import wres.config.yaml.components.LeadTimeInterval;
import wres.config.yaml.components.LeadTimeIntervalBuilder;
import wres.config.yaml.components.Source;
import wres.config.yaml.components.SourceBuilder;
import wres.config.yaml.components.SourceInterface;
import wres.config.yaml.components.TimeInterval;
import wres.config.yaml.components.TimeIntervalBuilder;
import wres.config.yaml.components.TimePools;
import wres.config.yaml.components.TimePoolsBuilder;
import wres.config.yaml.components.VariableBuilder;
import wres.datamodel.time.TimeSeries;
import wres.reading.DataSource;
import wres.reading.DataSource.DataDisposition;
import wres.reading.TimeSeriesTuple;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryTuple;

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

    @Disabled( "Until we can write a small grid to an in-memory file system, probably using the UCAR Netcdf API." )
    @Test
    void readTwentyFourShortRangeNwmForecastsForOneFeatureProducesTwentyFourTimeSeries()
    {
        // Read a vector source
        Path path = Paths.get( "data/nwmVector/" );

        Source fakeDeclarationSource =
                SourceBuilder.builder()
                             .uri( path.toUri() )
                             .sourceInterface( SourceInterface.NWM_SHORT_RANGE_CHANNEL_RT_CONUS )
                             .build();

        DataSource fakeSource = DataSource.of( DataDisposition.NETCDF_VECTOR,
                                               fakeDeclarationSource,
                                               DatasetBuilder.builder()
                                                             .type( DataType.SINGLE_VALUED_FORECASTS )
                                                             .sources( List.of( fakeDeclarationSource ) )
                                                             .variable( VariableBuilder.builder()
                                                                                       .name( "streamflow" )
                                                                                       .build() )
                                                             .build(),
                                               Collections.emptyList(),
                                               // Use a fake URI with an NWIS-like string as this is used to trigger the
                                               // identification of an instantaneous timescale
                                               path.toUri(),
                                               DatasetOrientation.RIGHT );

        LeadTimeInterval leadTimes = LeadTimeIntervalBuilder.builder()
                                                            .minimum( Duration.ofHours( 0 ) )
                                                            .maximum( Duration.ofHours( 18 ) )
                                                            .build();
        TimeInterval referenceDates = TimeIntervalBuilder.builder()
                                                         .minimum( Instant.parse( "2017-08-07T23:59:59Z" ) )
                                                         .maximum( Instant.parse( "2017-08-08T23:00:00Z" ) )
                                                         .build();
        TimeInterval validDates = TimeIntervalBuilder.builder()
                                                     .minimum( Instant.parse( "2017-08-07T23:59:59Z" ) )
                                                     .maximum( Instant.parse( "2017-08-09T17:00:00Z" ) )
                                                     .build();
        TimePools referenceTimePools = TimePoolsBuilder.builder()
                                                       .period( Duration.ofHours( 13 ) )
                                                       .frequency( Duration.ofHours( 7 ) )
                                                       .build();
        Set<GeometryTuple> geometries = Set.of( GeometryTuple.newBuilder()
                                                             .setLeft( Geometry.newBuilder().setName( "18384141" ) )
                                                             .setRight( Geometry.newBuilder().setName( "18384141" ) )
                                                             .build() );
        Features features = FeaturesBuilder.builder()
                                           .geometries( geometries )
                                           .build();
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .leadTimes( leadTimes )
                                                                        .referenceDates( referenceDates )
                                                                        .validDates( validDates )
                                                                        .referenceDatePools( referenceTimePools )
                                                                        .features( features )
                                                                        .build();

        NwmVectorReader reader = NwmVectorReader.of( declaration );

        List<TimeSeries<Double>> actual = reader.read( fakeSource )
                                                .map( TimeSeriesTuple::getSingleValuedTimeSeries )
                                                .toList();

        assertEquals( 24, actual.size() );
    }

}
