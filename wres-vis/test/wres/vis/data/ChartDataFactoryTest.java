package wres.vis.data;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import org.jfree.data.category.CategoryDataset;
import org.jfree.data.xy.XYDataset;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import wres.config.MetricConstants;
import wres.config.components.ThresholdOperator;
import wres.config.components.ThresholdOrientation;
import wres.datamodel.types.OneOrTwoDoubles;
import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.datamodel.statistics.DoubleScoreStatisticOuter.DoubleScoreComponentOuter;
import wres.datamodel.statistics.DurationDiagramStatisticOuter;
import wres.datamodel.statistics.DurationScoreStatisticOuter;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.MessageUtilities;
import wres.statistics.generated.BoxplotMetric;
import wres.statistics.generated.BoxplotStatistic;
import wres.statistics.generated.DiagramMetric;
import wres.statistics.generated.DiagramStatistic;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.Outputs.GraphicFormat.GraphicShape;
import wres.statistics.generated.Pool;
import wres.statistics.generated.TimeWindow;
import wres.vis.TestDataGenerator;

/**
 * Tests the {@link ChartDataFactory}.
 *
 * @author James Brown
 */

class ChartDataFactoryTest
{
    /** Double equality delta. */
    private static final double DELTA = 0.0001;

    private static final Instant FIRST_INSTANT = Instant.parse( "2521-12-15T12:00:00Z" );

    private static final Instant SECOND_INSTANT = Instant.parse( "2521-12-15T18:00:00Z" );

    private ChronoUnit durationUnits;

    @BeforeEach
    void runBeforeEachTest()
    {
        this.durationUnits = ChronoUnit.HOURS;
    }

    /**
     * Do not throw an {@link IndexOutOfBoundsException} when the input is empty. See #65503.
     */

    @Test
    void testOfBoxPlotOutputDoesNotThrowIndexOutOfBoundsExceptionForZeroBoxes()
    {
        PoolMetadata basicPool =
                PoolMetadata.of( PoolMetadata.of( Evaluation.newBuilder()
                                                            .setMeasurementUnit( MeasurementUnit.DIMENSIONLESS )
                                                            .build(),
                                                  Pool.getDefaultInstance() ),
                                 TimeWindowOuter.of( MessageUtilities.getTimeWindow() ) );

        BoxplotStatisticOuter input = BoxplotStatisticOuter.of( BoxplotStatistic.newBuilder()
                                                                                .setMetric( BoxplotMetric.newBuilder()
                                                                                                         .setName(
                                                                                                                 MetricName.BOX_PLOT_OF_ERRORS ) )
                                                                                .build(),
                                                                basicPool );

        Assertions.assertDoesNotThrow( () -> ChartDataFactory.ofBoxplotStatistics( List.of( input ),
                                                                                   ChronoUnit.SECONDS ) );
    }

    @Test
    void testOfDoubleScoreByPoolingWindowProducesXYDatasetWithTwoIssuedPoolsOnDomainAxis()
    {
        List<DoubleScoreComponentOuter> scores = TestDataGenerator.getScoresForTwoIssuedDatePools();
        XYDataset testData = ChartDataFactory.ofDoubleScoreByPoolingWindow( scores,
                                                                            this.durationUnits,
                                                                            GraphicShape.ISSUED_DATE_POOLS );

        double actualScoreOne = testData.getYValue( 0, 0 );
        assertEquals( 0.1, actualScoreOne, DELTA );
        assertEquals( FIRST_INSTANT.toEpochMilli(), testData.getXValue( 0, 0 ) );

        double actualScoreTwo = testData.getYValue( 0, 1 );
        assertEquals( 0.2, actualScoreTwo, DELTA );
        assertEquals( SECOND_INSTANT.toEpochMilli(), testData.getXValue( 0, 1 ) );
    }

    @Test
    void testOfDoubleScoreByPoolingWindowProducesXYDatasetWithTwoValidPoolsOnDomainAxis()
    {
        List<DoubleScoreComponentOuter> scores = TestDataGenerator.getScoresForTwoValidDatePools();
        XYDataset testData = ChartDataFactory.ofDoubleScoreByPoolingWindow( scores,
                                                                            this.durationUnits,
                                                                            GraphicShape.VALID_DATE_POOLS );

        double actualScoreOne = testData.getYValue( 0, 0 );
        assertEquals( 0.1, actualScoreOne, DELTA );
        assertEquals( FIRST_INSTANT.toEpochMilli(), testData.getXValue( 0, 0 ) );

        double actualScoreTwo = testData.getYValue( 0, 1 );
        assertEquals( 0.2, actualScoreTwo, DELTA );
        assertEquals( SECOND_INSTANT.toEpochMilli(), testData.getXValue( 0, 1 ) );
    }

    @Test
    void testOfDurationScoreSummaryStatisticsProducesCategoryDatasetWithSixStatistics()
    {
        List<DurationScoreStatisticOuter> scores = TestDataGenerator.getTimeToPeakErrorStatistics();
        CategoryDataset testData = ChartDataFactory.ofDurationScoreSummaryStatistics( scores );

        assertEquals( 6, testData.getColumnCount() );

        // Make an assertion about one of them, the minimum, -22 HOURS
        Number statistic = testData.getValue( "All data", "MINIMUM" );

        assertEquals( -22.0, statistic.doubleValue(), DELTA );
    }

    @Test
    void testOfDurationDiagramStatisticsProducesXYDatasetWithTenStatistics()
    {
        List<DurationDiagramStatisticOuter> statistics = TestDataGenerator.getTimeToPeakErrors();
        XYDataset testData = ChartDataFactory.ofDurationDiagramStatistics( statistics );

        assertEquals( 10, testData.getItemCount( 0 ) );

        // Make an assertion about one of them, the minimum, -22 HOURS
        Number actualTime = testData.getX( 0, 4 );

        Instant expectedTime = Instant.parse( "1985-01-05T00:00:00Z" );
        assertEquals( expectedTime.toEpochMilli(), actualTime.doubleValue(), DELTA );

        Number actualStatistic = testData.getY( 0, 4 );
        assertEquals( 8.0, actualStatistic.doubleValue(), DELTA );
    }

    @Test
    void testOfDoubleScoreByLeadAndThreshold()
    {
        DoubleScoreMetric.DoubleScoreMetricComponent metric
                = DoubleScoreMetric.DoubleScoreMetricComponent.newBuilder()
                                                              .setName( MetricName.MAIN )
                                                              .build();

        DoubleScoreStatistic.DoubleScoreStatisticComponent
                scoreOne = DoubleScoreStatistic.DoubleScoreStatisticComponent.newBuilder()
                                                                             .setMetric( metric )
                                                                             .setValue( 2.3 )
                                                                             .build();

        DoubleScoreStatistic.DoubleScoreStatisticComponent
                scoreTwo = DoubleScoreStatistic.DoubleScoreStatisticComponent.newBuilder()
                                                                             .setMetric( metric )
                                                                             .setValue( 2.7 )
                                                                             .build();

        DoubleScoreStatistic.DoubleScoreStatisticComponent
                scoreThree = DoubleScoreStatistic.DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( metric )
                                                                               .setValue( Double.POSITIVE_INFINITY )
                                                                               .build();

        TimeWindow timeWindow = TimeWindow.newBuilder()
                                          .setEarliestLeadDuration( Duration.newBuilder().setSeconds( 0 ) )
                                          .setLatestLeadDuration( Duration.newBuilder().setSeconds( 33 ) )
                                          .setEarliestReferenceTime( Timestamp.getDefaultInstance() )
                                          .setLatestReferenceTime( Timestamp.getDefaultInstance() )
                                          .setEarliestValidTime( Timestamp.getDefaultInstance() )
                                          .setLatestValidTime( Timestamp.getDefaultInstance() )
                                          .build();

        TimeWindow timeWindowTwo = timeWindow.toBuilder()
                                             .setLatestLeadDuration( Duration.newBuilder().setSeconds( 55 ) )
                                             .build();

        TimeWindow timeWindowThree = timeWindow.toBuilder()
                                               .setLatestLeadDuration( Duration.newBuilder().setSeconds( 77 ) )
                                               .build();

        OneOrTwoThresholds thresholds = OneOrTwoThresholds.of( ThresholdOuter.ALL_DATA );
        PoolMetadata metaOne = PoolMetadata.of( PoolMetadata.of(), TimeWindowOuter.of( timeWindow ), thresholds );
        PoolMetadata metaTwo = PoolMetadata.of( PoolMetadata.of(), TimeWindowOuter.of( timeWindowTwo ), thresholds );
        PoolMetadata metaThree =
                PoolMetadata.of( PoolMetadata.of(), TimeWindowOuter.of( timeWindowThree ), thresholds );

        DoubleScoreComponentOuter outerScoreOne = DoubleScoreComponentOuter.of( scoreOne, metaOne );
        DoubleScoreComponentOuter outerScoreTwo = DoubleScoreComponentOuter.of( scoreTwo, metaTwo );
        DoubleScoreComponentOuter outerScoreThree = DoubleScoreComponentOuter.of( scoreThree, metaThree );

        List<DoubleScoreComponentOuter> statistics = List.of( outerScoreOne, outerScoreTwo, outerScoreThree );

        XYDataset testDataset =
                ChartDataFactory.ofDoubleScoreByLeadAndThreshold( statistics, ChronoUnit.SECONDS );

        assertEquals( 3, testDataset.getItemCount( 0 ) );
        assertEquals( 1, testDataset.getSeriesCount() );
        assertEquals( 55.0, testDataset.getX( 0, 1 ) );
        assertEquals( 2.3, testDataset.getY( 0, 0 ) );
        assertEquals( Double.NaN, testDataset.getY( 0, 2 ) );
        assertEquals( ThresholdOuter.ALL_DATA.toString(), testDataset.getSeriesKey( 0 ) );
    }

    @Test
    void testOfDoubleScoreByThresholdAndLead()
    {
        DoubleScoreMetric.DoubleScoreMetricComponent metric
                = DoubleScoreMetric.DoubleScoreMetricComponent.newBuilder()
                                                              .setName( MetricName.MAIN )
                                                              .build();
        TimeWindow timeWindow = TimeWindow.newBuilder()
                                          .setEarliestLeadDuration( Duration.newBuilder().setSeconds( 0 ) )
                                          .setLatestLeadDuration( Duration.newBuilder().setSeconds( 33 ) )
                                          .setEarliestReferenceTime( Timestamp.getDefaultInstance() )
                                          .setLatestReferenceTime( Timestamp.getDefaultInstance() )
                                          .setEarliestValidTime( Timestamp.getDefaultInstance() )
                                          .setLatestValidTime( Timestamp.getDefaultInstance() )
                                          .build();

        DoubleScoreStatistic.DoubleScoreStatisticComponent
                scoreTwoOne = DoubleScoreStatistic.DoubleScoreStatisticComponent.newBuilder()
                                                                                .setMetric( metric )
                                                                                .setValue( 2.3 )
                                                                                .build();

        DoubleScoreStatistic.DoubleScoreStatisticComponent
                scoreTwoTwo = DoubleScoreStatistic.DoubleScoreStatisticComponent.newBuilder()
                                                                                .setMetric( metric )
                                                                                .setValue( 2.7 )
                                                                                .build();

        ThresholdOuter first = ThresholdOuter.of( OneOrTwoDoubles.of( 23.0 ),
                                                  ThresholdOperator.GREATER,
                                                  ThresholdOrientation.OBSERVED );
        ThresholdOuter second = ThresholdOuter.of( OneOrTwoDoubles.of( 29.0 ),
                                                   ThresholdOperator.GREATER,
                                                   ThresholdOrientation.OBSERVED );
        PoolMetadata metaTwoOne =
                PoolMetadata.of( PoolMetadata.of(), TimeWindowOuter.of( timeWindow ), OneOrTwoThresholds.of( first ) );
        PoolMetadata metaTwoTwo =
                PoolMetadata.of( PoolMetadata.of(), TimeWindowOuter.of( timeWindow ), OneOrTwoThresholds.of( second ) );

        DoubleScoreComponentOuter outerScoreTwoOne = DoubleScoreComponentOuter.of( scoreTwoOne, metaTwoOne );
        DoubleScoreComponentOuter outerScoreTwoTwo = DoubleScoreComponentOuter.of( scoreTwoTwo, metaTwoTwo );

        List<DoubleScoreComponentOuter> statistics = List.of( outerScoreTwoOne, outerScoreTwoTwo );

        XYDataset testDataset =
                ChartDataFactory.ofDoubleScoreByThresholdAndLead( statistics, ChronoUnit.SECONDS );

        assertEquals( 2, testDataset.getItemCount( 0 ) );
        assertEquals( 1, testDataset.getSeriesCount() );
        assertEquals( 29.0, testDataset.getX( 0, 1 ) );
        assertEquals( 2.3, testDataset.getY( 0, 0 ) );
        assertEquals( "33", testDataset.getSeriesKey( 0 ) );
    }

    @Test
    void testOfDiagramStatisticsByLeadAndThreshold()
    {
        DiagramMetric.DiagramMetricComponent observedQuantiles =
                DiagramMetric.DiagramMetricComponent.newBuilder()
                                                    .setName( MetricName.OBSERVED_QUANTILES )
                                                    .build();

        DiagramMetric.DiagramMetricComponent predictedQuantiles =
                DiagramMetric.DiagramMetricComponent.newBuilder()
                                                    .setName( MetricName.PREDICTED_QUANTILES )
                                                    .build();

        DiagramMetric metric = DiagramMetric.newBuilder()
                                            .addComponents( observedQuantiles )
                                            .addComponents( predictedQuantiles )
                                            .setName( MetricName.QUANTILE_QUANTILE_DIAGRAM )
                                            .build();

        List<Double> observedQ = new ArrayList<>();
        List<Double> predictedQ = new ArrayList<>();

        for ( int i = 1; i < 11; i++ )
        {
            observedQ.add( ( double ) i );
            predictedQ.add( ( double ) i );
        }

        DiagramStatistic.DiagramStatisticComponent oqs =
                DiagramStatistic.DiagramStatisticComponent.newBuilder()
                                                          .setMetric( observedQuantiles )
                                                          .addAllValues( observedQ )
                                                          .build();

        DiagramStatistic.DiagramStatisticComponent pqs =
                DiagramStatistic.DiagramStatisticComponent.newBuilder()
                                                          .setMetric( predictedQuantiles )
                                                          .addAllValues( predictedQ )
                                                          .build();

        DiagramStatistic diagram = DiagramStatistic.newBuilder()
                                                   .setMetric( metric )
                                                   .addStatistics( oqs )
                                                   .addStatistics( pqs )
                                                   .build();

        // Set metadata with minimum content
        TimeWindow timeWindow = TimeWindow.newBuilder()
                                          .setEarliestLeadDuration( Duration.newBuilder().setSeconds( 0 ) )
                                          .setLatestLeadDuration( Duration.newBuilder().setSeconds( 33 ) )
                                          .setEarliestReferenceTime( Timestamp.getDefaultInstance() )
                                          .setLatestReferenceTime( Timestamp.getDefaultInstance() )
                                          .setEarliestValidTime( Timestamp.getDefaultInstance() )
                                          .setLatestValidTime( Timestamp.getDefaultInstance() )
                                          .build();

        OneOrTwoThresholds thresholds = OneOrTwoThresholds.of( ThresholdOuter.ALL_DATA );
        PoolMetadata meta = PoolMetadata.of( PoolMetadata.of(), TimeWindowOuter.of( timeWindow ), thresholds );

        DiagramStatisticOuter outerDiagram = DiagramStatisticOuter.of( diagram, meta );

        XYDataset dataset = ChartDataFactory.ofDiagramStatisticsByLeadAndThreshold( List.of( outerDiagram ),
                                                                                    MetricConstants.MetricDimension.PREDICTED_QUANTILES,
                                                                                    MetricConstants.MetricDimension.OBSERVED_QUANTILES,
                                                                                    ChronoUnit.SECONDS );

        assertEquals( 10, dataset.getItemCount( 0 ) );
        assertEquals( 1, dataset.getSeriesCount() );
        assertEquals( 5.0, dataset.getX( 0, 4 ) );
        assertEquals( 7.0, dataset.getY( 0, 6 ) );
        assertEquals( ThresholdOuter.ALL_DATA.toString(), dataset.getSeriesKey( 0 ) );
    }
}
