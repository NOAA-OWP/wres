package wres.vis;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;
import org.jfree.chart.JFreeChart;
import org.jfree.data.xy.XYDataset;

import ohd.hseb.charter.ChartConstants;
import ohd.hseb.charter.datasource.XYChartDataSourceException;
import ohd.hseb.charter.datasource.instances.CategoricalXYChartDataSource;
import ohd.hseb.charter.parameters.SeriesDrawingParameters;
import wres.datamodel.MetricConstants;
import wres.datamodel.Threshold;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.DurationScoreOutput;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;

/**
 * Top-level data source to use for categorical plots.  Call the static methods to generate instances for different
 * types of input.  
 * @author Hank.Herr
 *
 * @param <T> the input data type
 */
public abstract class WRESCategoricalChartDataSource<T> extends CategoricalXYChartDataSource
{

    private final T input;

    private WRESCategoricalChartDataSource( final int orderIndex,
                                            final T input,
                                            final String[] xCategories,
                                            final List<double[]> yAxisValuesBySeries )
            throws XYChartDataSourceException
    {
        super( null, orderIndex, xCategories, yAxisValuesBySeries );

        Objects.requireNonNull( input, "Specify a non-null input dataset for building the chart data source." );
        this.input = input;
        buildInitialParameters( orderIndex, yAxisValuesBySeries.size() );
        this.setXAxisType( ChartConstants.AXIS_IS_CATEGORICAL );
        this.setComputedDataType( ChartConstants.AXIS_IS_NUMERICAL );
    }


    private void buildInitialParameters( final int dataSourceOrderIndex, final int numberOfSeries )
    {
        getDefaultFullySpecifiedDataSourceDrawingParameters().setDataSourceOrderIndex( dataSourceOrderIndex );
        getDefaultFullySpecifiedDataSourceDrawingParameters().setPlotterName( "Bar" );
        getDefaultFullySpecifiedDataSourceDrawingParameters().setSubPlotIndex( 0 );
        getDefaultFullySpecifiedDataSourceDrawingParameters().setYAxisIndex( 0 );
        getDefaultFullySpecifiedDataSourceDrawingParameters().setDefaultDomainAxisTitle( "INSERT DEFAULT DOMAIN AXIS TITLE HERE!" );
        getDefaultFullySpecifiedDataSourceDrawingParameters().setDefaultRangeAxisTitle( "INSERT DEFAULT RANGE AXIS TITLE HERE!" );

        constructAllSeriesDrawingParameters( numberOfSeries );
        for ( int i = 0; i < numberOfSeries; i++ )
        {
            final SeriesDrawingParameters seriesParms =
                    getDefaultFullySpecifiedDataSourceDrawingParameters().getSeriesDrawingParametersForSeriesIndex( i );
            seriesParms.setupDefaultParameters();
            seriesParms.setNameInLegend( "" );
        }
    }

    protected T getInput()
    {
        return input;
    }

    /**
     * @return An instance of {@link XYDataset} to use in building the {@link JFreeChart}.
     */
    protected abstract XYDataset instantiateXYDataset();

    /**
     * Creates an instance for duration score output; i.e., summary stats of the time-to-peak errors.
     * @param orderIndex The data source order index.
     * @param input The input required for this of method.
     * @return An instance of {@link WRESCategoricalChartDataSource}.
     * @throws XYChartDataSourceException if the data source could not be created
     */
    public static WRESCategoricalChartDataSource<MetricOutputMapByTimeAndThreshold<DurationScoreOutput>>
            of( int orderIndex,
                MetricOutputMapByTimeAndThreshold<DurationScoreOutput> input )
                    throws XYChartDataSourceException
    {
        String[] xCategories = null;
        List<double[]> yAxisValuesBySeries = new ArrayList<>();
        boolean populateCategories = false;
        for ( Entry<Pair<TimeWindow, Threshold>, DurationScoreOutput> entry : input.entrySet() )
        {
            if ( xCategories == null )
            {
                xCategories = new String[entry.getValue().getComponents().size()];
                populateCategories = true;
            }
            else
            {
                populateCategories = false;
            }
            double[] yValues = new double[xCategories.length];

            DurationScoreOutput output = entry.getValue();
            int index = 0;
            for ( MetricConstants metric : output.getComponents() )
            {
                if ( populateCategories )
                {
                    xCategories[index] = metric.name();
                }
                else
                {
                    if ( !xCategories[index].equals( metric.name() ) )
                    {
                        throw new IllegalArgumentException( "The named categories are not consistent across all provided input." );
                    }
                }

                Duration durationStat = output.getComponent( metric ).getData();
                yValues[index] = durationStat.toHours();
                index++;
            }

            yAxisValuesBySeries.add( yValues );
            populateCategories = false;
        }

        //Creates the source.
        WRESCategoricalChartDataSource<MetricOutputMapByTimeAndThreshold<DurationScoreOutput>> source = new WRESCategoricalChartDataSource<MetricOutputMapByTimeAndThreshold<DurationScoreOutput>>( orderIndex,
                                                                                                           input,
                                                                                                           xCategories,
                                                                                                           yAxisValuesBySeries )
        {
            @Override
            public WRESCategoricalChartDataSource<MetricOutputMapByTimeAndThreshold<DurationScoreOutput>> returnNewInstanceWithCopyOfInitialParameters() throws XYChartDataSourceException
            {
                return of(getDataSourceOrderIndex(), getInput());
            }
            
            @Override
            protected XYDataset instantiateXYDataset()
            {
                return this.getXYDataSet();
            }
        };

        //Some appearance options specific to the input provided.
        source.getDefaultFullySpecifiedDataSourceDrawingParameters().setDefaultDomainAxisTitle( "@metricName@" );
        source.getDefaultFullySpecifiedDataSourceDrawingParameters().setDefaultRangeAxisTitle( "@metricShortName@@metricComponentNameSuffix@@outputUnitsLabelSuffix@" );
        WRESTools.applyDefaultJFreeChartColorSequence( source.getDefaultFullySpecifiedDataSourceDrawingParameters() );
        WRESTools.applyDefaultJFreeChartShapeSequence( source.getDefaultFullySpecifiedDataSourceDrawingParameters() );
        
        return source;
    }
}
