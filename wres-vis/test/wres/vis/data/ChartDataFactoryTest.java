package wres.vis.data;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import org.jfree.data.category.CategoryDataset;
import org.jfree.data.xy.XYDataset;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.datamodel.statistics.DoubleScoreStatisticOuter.DoubleScoreComponentOuter;
import wres.datamodel.statistics.DurationDiagramStatisticOuter;
import wres.datamodel.statistics.DurationScoreStatisticOuter;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.MessageFactory;
import wres.statistics.generated.BoxplotMetric;
import wres.statistics.generated.BoxplotStatistic;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.Outputs.GraphicFormat.GraphicShape;
import wres.statistics.generated.Pool;
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
                                 TimeWindowOuter.of( MessageFactory.getTimeWindow() ) );

        BoxplotStatisticOuter input = BoxplotStatisticOuter.of( BoxplotStatistic.newBuilder()
                                                                                .setMetric( BoxplotMetric.newBuilder()
                                                                                                         .setName( MetricName.BOX_PLOT_OF_ERRORS ) )
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
    
}
