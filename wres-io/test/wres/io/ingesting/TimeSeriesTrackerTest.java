package wres.io.ingesting;

import java.time.Instant;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;

import wres.config.yaml.components.DataType;
import wres.config.yaml.components.DatasetOrientation;
import wres.datamodel.types.Ensemble;
import wres.datamodel.space.Feature;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.reading.DataSource;
import wres.reading.TimeSeriesTuple;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.ReferenceTime;

/**
 * Tests the {@link TimeSeriesTracker}.
 *
 * @author James Brown
 */
class TimeSeriesTrackerTest
{
    @Test
    void testApplyTracksExpectedDataTypeForEachDatasetOrientation()
    {
        TimeSeriesTracker tracker = TimeSeriesTracker.of();

        Map<ReferenceTime.ReferenceTimeType, Instant> leftReferenceTimes =
                Map.of( ReferenceTime.ReferenceTimeType.ANALYSIS_START_TIME, Instant.MIN );
        TimeSeriesMetadata leftMetadata =
                new TimeSeriesMetadata.Builder().setReferenceTimes( leftReferenceTimes )
                                                .setVariableName( "foo" )
                                                .setFeature( Feature.of( Geometry.newBuilder()
                                                                                 .build() ) )
                                                .setUnit( "bar" )
                                                .build();

        TimeSeries<Double> leftSeries = TimeSeries.of( leftMetadata, new TreeSet<>() );

        DataSource leftSource = Mockito.mock( DataSource.class );
        Mockito.when( leftSource.getDatasetOrientation() )
               .thenReturn( DatasetOrientation.LEFT );
        TimeSeriesTuple leftTuple = TimeSeriesTuple.of( leftSeries, null, leftSource );

        DataSource rightSource = Mockito.mock( DataSource.class );
        Mockito.when( rightSource.getDatasetOrientation() )
               .thenReturn( DatasetOrientation.RIGHT );
        TimeSeriesMetadata rightMetadata =
                new TimeSeriesMetadata.Builder().setVariableName( "foo" )
                                                .setFeature( Feature.of( Geometry.newBuilder()
                                                                                 .build() ) )
                                                .setReferenceTimes( Map.of() )
                                                .setUnit( "bar" )
                                                .build();
        TimeSeries<Ensemble> rightSeries = TimeSeries.of( rightMetadata, new TreeSet<>() );
        TimeSeriesTuple rightTuple = TimeSeriesTuple.of( null, rightSeries, rightSource );

        DataSource baselineSource = Mockito.mock( DataSource.class );
        Mockito.when( baselineSource.getDatasetOrientation() )
               .thenReturn( DatasetOrientation.BASELINE );
        Map<ReferenceTime.ReferenceTimeType, Instant> baselineReferenceTimes =
                Map.of( ReferenceTime.ReferenceTimeType.T0, Instant.MIN );
        TimeSeriesMetadata baselineMetadata =
                new TimeSeriesMetadata.Builder().setReferenceTimes( baselineReferenceTimes )
                                                .setVariableName( "foo" )
                                                .setFeature( Feature.of( Geometry.newBuilder()
                                                                                 .build() ) )
                                                .setUnit( "bar" )
                                                .build();
        TimeSeries<Double> baselineSeries = TimeSeries.of( baselineMetadata, new TreeSet<>() );
        TimeSeriesTuple baselineTuple = TimeSeriesTuple.of( baselineSeries, null, baselineSource );

        tracker.apply( leftTuple );
        tracker.apply( rightTuple );
        tracker.apply( baselineTuple );

        Map<DatasetOrientation, DataType> expected
                = Map.of( DatasetOrientation.LEFT, DataType.ANALYSES,
                          DatasetOrientation.RIGHT, DataType.ENSEMBLE_FORECASTS,
                          DatasetOrientation.BASELINE, DataType.SINGLE_VALUED_FORECASTS );

        Map<DatasetOrientation, DataType> actual = tracker.getDataTypes()
                                                          .entrySet()
                                                          .stream()
                                                          .collect( Collectors.toMap( e -> e.getKey()
                                                                                            .getDatasetOrientation(),
                                                                                      Map.Entry::getValue ) );

        assertEquals( expected, actual );
    }
}
