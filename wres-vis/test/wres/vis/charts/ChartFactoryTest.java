package wres.vis.charts;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.XYPlot;
import org.jfree.data.category.CategoryDataset;
import org.jfree.data.xy.XYDataset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import wres.config.MetricConstants;
import wres.config.yaml.components.DatasetOrientation;
import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.statistics.DurationDiagramStatisticOuter;
import wres.datamodel.statistics.DurationScoreStatisticOuter;
import wres.datamodel.statistics.PairsStatisticOuter;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.generated.Outputs.GraphicFormat.GraphicShape;
import wres.vis.TestDataGenerator;

/**
 * Tests the {@link ChartFactory}. Favors high-level assertions about the general shape of the returned charts, rather
 * than low-level assertions about the chart instances.
 *
 * @author James Brown
 */
class ChartFactoryTest
{
    /** Chart factory instance. */
    private ChartFactory chartFactory;

    /**
     * Generate a plot by lead time on the domain axis.
     */
    @BeforeEach
    void runBeforeEachTest()
    {
        this.chartFactory = ChartFactory.of();
    }

    @Test
    void testGetScoreChartsProducesOneChartWithFourSeriesAndTenItems()
    {
        List<DoubleScoreStatisticOuter> statistics = TestDataGenerator.getScoresForIssuedTimeAndLeadDurationPools();

        Map<MetricConstants, JFreeChart> charts = this.chartFactory.getScoreCharts( statistics,
                                                                                    GraphicShape.ISSUED_DATE_POOLS,
                                                                                    ChronoUnit.HOURS );


        assertEquals( 1, charts.size() );

        assertTrue( charts.containsKey( MetricConstants.MAIN ) );

        JFreeChart chart = charts.get( MetricConstants.MAIN );

        XYDataset dataset = chart.getXYPlot()
                                 .getDataset();

        assertEquals( 4, dataset.getSeriesCount() );
        assertEquals( 10, dataset.getItemCount( 0 ) );
    }

    @Test
    void testGetScoreChartsProducesOneChartWithOneSeriesAndTwentyFourItems()
    {
        List<DoubleScoreStatisticOuter> statistics = TestDataGenerator.getScoresForIssuedTimePools();

        Map<MetricConstants, JFreeChart> charts = this.chartFactory.getScoreCharts( statistics,
                                                                                    GraphicShape.ISSUED_DATE_POOLS,
                                                                                    ChronoUnit.HOURS );


        assertEquals( 1, charts.size() );

        assertTrue( charts.containsKey( MetricConstants.MAIN ) );

        JFreeChart chart = charts.get( MetricConstants.MAIN );

        XYDataset dataset = chart.getXYPlot()
                                 .getDataset();

        assertEquals( 1, dataset.getSeriesCount() );
        assertEquals( 24, dataset.getItemCount( 0 ) );
    }

    @Test
    void testGetDiagramChartByLeadAndThresholdProducesThreeChartsWithTwoSeries()
    {
        List<DiagramStatisticOuter> statistics =
                TestDataGenerator.getDiagramStatisticsForTwoThresholdsAndThreeLeadDurations();

        Map<Object, JFreeChart> charts = this.chartFactory.getDiagramCharts( statistics,
                                                                             GraphicShape.LEAD_THRESHOLD,
                                                                             ChronoUnit.HOURS );

        assertEquals( 3, charts.size() );

        for ( Map.Entry<Object, JFreeChart> next : charts.entrySet() )
        {
            JFreeChart nextChart = next.getValue();
            XYPlot nextPlot = nextChart.getXYPlot();
            XYDataset nextDataset = nextPlot.getDataset();

            assertEquals( 2, nextDataset.getSeriesCount() );
        }
    }

    @Test
    void testGetDiagramChartByThresholdAndLeadProducesTwoChartsWithThreeSeries()
    {
        List<DiagramStatisticOuter> statistics =
                TestDataGenerator.getDiagramStatisticsForTwoThresholdsAndThreeLeadDurations();

        Map<Object, JFreeChart> charts = this.chartFactory.getDiagramCharts( statistics,
                                                                             GraphicShape.THRESHOLD_LEAD,
                                                                             ChronoUnit.HOURS );

        assertEquals( 2, charts.size() );

        for ( Map.Entry<Object, JFreeChart> next : charts.entrySet() )
        {
            JFreeChart nextChart = next.getValue();
            XYPlot nextPlot = nextChart.getXYPlot();
            XYDataset nextDataset = nextPlot.getDataset();

            assertEquals( 3, nextDataset.getSeriesCount() );
        }
    }

    @Test
    void testGetDurationScoreChartProducesOneChartWithSixStatistics()
    {
        List<DurationScoreStatisticOuter> statistics = TestDataGenerator.getTimeToPeakErrorStatistics();

        JFreeChart chart = this.chartFactory.getDurationScoreChart( statistics,
                                                                    ChronoUnit.HOURS );

        CategoryDataset dataset = chart.getCategoryPlot()
                                       .getDataset();

        assertEquals( 1, dataset.getRowCount() );
        assertEquals( 6, dataset.getColumnCount() );
    }

    @Test
    void testGetDurationDiagramChartProducesOneChartWithTenStatistics()
    {
        List<DurationDiagramStatisticOuter> statistics = TestDataGenerator.getTimeToPeakErrors();

        JFreeChart chart = this.chartFactory.getDurationDiagramChart( statistics,
                                                                      ChronoUnit.HOURS );

        XYDataset dataset = chart.getXYPlot()
                                 .getDataset();

        assertEquals( 1, dataset.getSeriesCount() );
        assertEquals( 10, dataset.getItemCount( 0 ) );
    }

    @Test
    void testGetBoxplotChartProducesOneChartWithTwoBoxesAndFiveQuantiles()
    {
        List<BoxplotStatisticOuter> statistics = TestDataGenerator.getBoxPlotPerPoolForTwoPools();

        JFreeChart chart = this.chartFactory.getBoxplotChart( statistics, ChronoUnit.HOURS );

        XYDataset dataset = chart.getXYPlot()
                                 .getDataset();

        assertEquals( 5, dataset.getSeriesCount() ); // Quantiles
        assertEquals( 2, dataset.getItemCount( 0 ) ); // Boxes
    }

    @Test
    void testGetBoxplotChartPerPoolProducesTwoChartsWithOneBoxAndFiveQuantiles()
    {
        List<BoxplotStatisticOuter> statistics = TestDataGenerator.getBoxPlotPerPairForOnePool();

        Map<TimeWindowOuter, JFreeChart> charts =
                this.chartFactory.getBoxplotChartPerPool( statistics, ChronoUnit.HOURS );

        assertEquals( 1, charts.size() );

        for ( Map.Entry<TimeWindowOuter, JFreeChart> next : charts.entrySet() )
        {
            JFreeChart nextChart = next.getValue();
            XYPlot nextPlot = nextChart.getXYPlot();
            XYDataset dataset = nextPlot.getDataset();

            assertEquals( 5, dataset.getSeriesCount() ); // Quantiles
            assertEquals( 3, dataset.getItemCount( 0 ) ); // Boxes
        }
    }

    @Test
    void testGetPairsChartProducesOneChartWithFourSeries()
    {
        PairsStatisticOuter statistics = TestDataGenerator.getPairsStatisticsForOnePoolWithTwoTimeSeries();

        JFreeChart chart = this.chartFactory.getPairsChart( statistics, ChronoUnit.HOURS );

        XYDataset dataset = chart.getXYPlot()
                                 .getDataset();

        assertAll( () -> assertEquals( 4, dataset.getSeriesCount() ),
                   () -> assertEquals( DatasetOrientation.LEFT
                                       + GraphicsUtils.PAIR_THEME_SEPARATOR
                                       + 1,
                                       dataset.getSeriesKey( 0 ) ),
                   () -> assertEquals( DatasetOrientation.RIGHT
                                       + GraphicsUtils.PAIR_THEME_SEPARATOR
                                       + 1,
                                       dataset.getSeriesKey( 1 ) ),
                   () -> assertEquals( DatasetOrientation.LEFT
                                       + GraphicsUtils.PAIR_THEME_SEPARATOR
                                       + 2,
                                       dataset.getSeriesKey( 2 ) ),
                   () -> assertEquals( DatasetOrientation.RIGHT
                                       + GraphicsUtils.PAIR_THEME_SEPARATOR
                                       + 2,
                                       dataset.getSeriesKey( 3 ) ),
                   () -> assertEquals( DatasetOrientation.LEFT.toString(),
                                       chart.getLegend()
                                            .getSources()[0]
                                               .getLegendItems()
                                               .get( 0 )
                                               .getLabel() ),
                   () -> assertEquals( DatasetOrientation.RIGHT.toString(),
                                       chart.getLegend()
                                            .getSources()[0]
                                               .getLegendItems()
                                               .get( 1 )
                                               .getLabel() ) );

    }

}
