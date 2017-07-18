package wres.vis;

import java.util.Objects;

import org.jfree.data.xy.XYDataset;

import com.google.common.base.Strings;

import ohd.hseb.charter.ChartConstants;
import ohd.hseb.charter.datasource.DefaultXYChartDataSource;
import ohd.hseb.charter.datasource.XYChartDataSource;
import ohd.hseb.charter.datasource.XYChartDataSourceException;
import ohd.hseb.charter.parameters.DataSourceDrawingParameters;
import ohd.hseb.charter.parameters.SeriesDrawingParameters;
import wres.datamodel.metric.MetricInput;
import wres.datamodel.metric.MetricOutputMapByLeadThreshold;
import wres.datamodel.metric.ScalarOutput;

/**
 * A chart data source for a single verification metric, indexed by threshold and forecast lead time.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public class ScalarOutputByThresholdLeadXYChartDataSource extends DefaultXYChartDataSource
{
    /**
     * The data.
     */

    private final MetricOutputMapByLeadThreshold<ScalarOutput> input;

    /**
     * @param orderIndex The data source order index within the plotted chart.
     * @param input The {@link MetricInput} for which to display a chart.
     */
    public ScalarOutputByThresholdLeadXYChartDataSource(final int orderIndex,
                                                        final MetricOutputMapByLeadThreshold<ScalarOutput> input)
    {
        Objects.requireNonNull(input, "Specify a non-null input dataset for building the chart data source.");
        this.input = input;
        buildInitialParameters(orderIndex);
        this.setXAxisType(ChartConstants.AXIS_IS_NUMERICAL);
        this.setComputedDataType(ChartConstants.AXIS_IS_NUMERICAL);
    }

    /**
     * Called in the constructor, it calls {@link #constructAllSeriesDrawingParameters(int)} and initializes the data
     * source drawing parameters.
     */
    private void buildInitialParameters(final int dataSourceOrderIndex)
    {
        getDefaultFullySpecifiedDataSourceDrawingParameters().setDataSourceOrderIndex(dataSourceOrderIndex);
        getDefaultFullySpecifiedDataSourceDrawingParameters().setPlotterName("LineAndScatter");
        getDefaultFullySpecifiedDataSourceDrawingParameters().setSubPlotIndex(0);
        getDefaultFullySpecifiedDataSourceDrawingParameters().setYAxisIndex(0);

        //TODO Need to ensure that the arguments used below are standard arguments created in the factory that generates images!
        getDefaultFullySpecifiedDataSourceDrawingParameters().setDefaultDomainAxisTitle("THRESHOLD VALUE@inputUnitsText@");
        getDefaultFullySpecifiedDataSourceDrawingParameters().setDefaultRangeAxisTitle("@metricShortName@@outputUnitsText@");

        final int seriesCount = input.keySetByLead().size();
        constructAllSeriesDrawingParameters(seriesCount);

        for(int i = 0; i < seriesCount; i++)
        {
            final SeriesDrawingParameters seriesParms =
                                                      getDefaultFullySpecifiedDataSourceDrawingParameters().getSeriesDrawingParametersForSeriesIndex(i);
            seriesParms.setupDefaultParameters();
        }
    }

    @Override
    public XYChartDataSource returnNewInstanceWithCopyOfInitialParameters() throws XYChartDataSourceException
    {
        final ScalarOutputByThresholdLeadXYChartDataSource copy =
                                                                new ScalarOutputByThresholdLeadXYChartDataSource(getDataSourceOrderIndex(),
                                                                                                                 input);
        copy.copyTheseParametersIntoDataSource(this);
        return copy;
    }

    @Override
    protected XYDataset buildXYDataset(final DataSourceDrawingParameters parameters) throws XYChartDataSourceException
    {
        //Legend items are thresholds, and they are fully defined in the input data source, so an override is unlikely 
        //to make sense here. 
        final ScalarOutputByThresholdLeadXYDataset dataSet = new ScalarOutputByThresholdLeadXYDataset(input);

        //Set the legend names based on the passed in parameters, which are fully processed.
        //Legend names are set in the dataSet itself, which is why this must be done when the dataSet is created.
        //I know... I don't like it either.
        for(int i = 0; i < dataSet.getSeriesCount(); i++)
        {
            if(!Strings.isNullOrEmpty(parameters.getSeriesDrawingParametersForSeriesIndex(i).getNameInLegend()))
            {
                dataSet.setLegendName(i,
                                      parameters.getArguments()
                                                .replaceArgumentsInString(parameters.getSeriesDrawingParametersForSeriesIndex(i)
                                                                                    .getArgumentReplacedNameInLegend()));
            }
        }

        return dataSet;
    }

}
