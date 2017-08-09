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
import wres.datamodel.metric.MetricOutputMapByLeadThreshold;
import wres.datamodel.metric.MultiVectorOutput;

public class ReliabilityDiagramXYChartDataSource extends DefaultXYChartDataSource
{
    
    private final MetricOutputMapByLeadThreshold<MultiVectorOutput> input;

    public ReliabilityDiagramXYChartDataSource(final int orderIndex,
                                                        final MetricOutputMapByLeadThreshold<MultiVectorOutput> input)
    {
        Objects.requireNonNull(input, "Specify a non-null input dataset for building the chart data source.");
        this.input = input;
        buildInitialParameters(orderIndex);
        this.setXAxisType(ChartConstants.AXIS_IS_NUMERICAL);
        this.setComputedDataType(ChartConstants.AXIS_IS_NUMERICAL);
    }

    private void buildInitialParameters(final int dataSourceOrderIndex)
    {
        getDefaultFullySpecifiedDataSourceDrawingParameters().setDataSourceOrderIndex(dataSourceOrderIndex);
        getDefaultFullySpecifiedDataSourceDrawingParameters().setPlotterName("LineAndScatter");
        getDefaultFullySpecifiedDataSourceDrawingParameters().setSubPlotIndex(0);
        getDefaultFullySpecifiedDataSourceDrawingParameters().setYAxisIndex(0);

        //TODO Need to ensure that the arguments used below are standard arguments created in the factory that generates images!
        getDefaultFullySpecifiedDataSourceDrawingParameters().setDefaultDomainAxisTitle("Forecast Probability");
        getDefaultFullySpecifiedDataSourceDrawingParameters().setDefaultRangeAxisTitle("Observed Probability Given Forecast Probability");

        final int seriesCount = input.keySetByThreshold().size();
        constructAllSeriesDrawingParameters(seriesCount);

        for(int i = 0; i < seriesCount; i++)
        {
            final SeriesDrawingParameters seriesParms =
                                                      getDefaultFullySpecifiedDataSourceDrawingParameters().getSeriesDrawingParametersForSeriesIndex(i);
            seriesParms.setupDefaultParameters();
            seriesParms.setNameInLegend("");
        }
    }

    @Override
    public XYChartDataSource returnNewInstanceWithCopyOfInitialParameters() throws XYChartDataSourceException
    {
        final ReliabilityDiagramXYChartDataSource copy =
                                                                new ReliabilityDiagramXYChartDataSource(getDataSourceOrderIndex(),
                                                                                                                 input);
        copy.copyTheseParametersIntoDataSource(this);
        return copy;
    }

    @Override
    protected XYDataset buildXYDataset(final DataSourceDrawingParameters parameters) throws XYChartDataSourceException
    {
        //Legend items are lead times, and they are fully defined in the input data source, so an override is unlikely 
        //to make sense here. 
        final ReliabilityDiagramXYDataset dataSet = new ReliabilityDiagramXYDataset(input);

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
