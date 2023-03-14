package wres.vis.charts;

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
import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.statistics.DurationDiagramStatisticOuter;
import wres.datamodel.statistics.DurationScoreStatisticOuter;
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
    void getScoreChartsProducesOneChartWithFourSeriesAndTenItems()
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
    void getScoreChartsProducesOneChartWithOneSeriesAndTwentyFourItems()
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
    void getDiagramChartByLeadAndThresholdProducesThreeChartsWithTwoSeries()
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
    void getDiagramChartByThresholdAndLeadProducesTwoChartsWithThreeSeries()
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
    void getDurationScoreChartProducesOneChartWithSixStatistics()
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
    void getDurationDiagramChartProducesOneChartWithTenStatistics()
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
}
