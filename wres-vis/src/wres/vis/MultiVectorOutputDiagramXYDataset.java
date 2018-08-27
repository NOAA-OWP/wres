package wres.vis;

import java.util.SortedSet;

import org.jfree.data.xy.XYDataset;

import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.Slicer;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.statistics.ListOfStatistics;
import wres.datamodel.statistics.MultiVectorStatistic;
import wres.datamodel.thresholds.OneOrTwoThresholds;

/**
 * The {@link XYDataset} for use in building a chart that plots a {@link MultiVectorStatistic}.
 * 
 * @author Hank.Herr
 */
public class MultiVectorOutputDiagramXYDataset extends WRESAbstractXYDataset<ListOfStatistics<MultiVectorStatistic>, ListOfStatistics<MultiVectorStatistic>>
{
    private static final long serialVersionUID = 4254109136599641286L;
    private final MetricDimension xConstant;
    private final MetricDimension yConstant;

    public MultiVectorOutputDiagramXYDataset(final ListOfStatistics<MultiVectorStatistic> input, final MetricDimension xConstant, final MetricDimension yConstant)
    {
        super(input);
        this.xConstant = xConstant;
        this.yConstant = yConstant;
    }

    @Override
    protected void preparePlotData(final ListOfStatistics<MultiVectorStatistic> rawData)
    {
        //This check should not be necessary, since the conditions should be impossible.  I'll do it anyway just to be sure.
        if ( rawData.getData().isEmpty() )
        {
            throw new IllegalArgumentException( "Specify non-empty input." );
        }

        setPlotData(rawData);
    }

    @Override
    public int getItemCount(final int series)
    {
        return getPlotData().getData()
                            .get(series)
                            .getData()
                            .get(xConstant)
                            .getDoubles().length;
    }

    @Override
    public Number getX(final int series, final int item)
    {
        return getPlotData().getData()
                            .get(series)
                            .getData()
                            .get(xConstant)
                            .getDoubles()[item];
    }

    @Override
    public Number getY(final int series, final int item)
    {
        return getPlotData().getData()
                            .get(series)
                            .getData()
                            .get(yConstant)
                            .getDoubles()[item];
    }

    @Override
    public int getSeriesCount()
    {
        return getPlotData().getData().size();
    }

    @Override
    public Comparable<String> getSeriesKey(final int series)
    {
        if (isLegendNameOverridden(series))
        {
            return getOverrideLegendName(series);
        }
        
        SortedSet<TimeWindow> timeWindows =
                Slicer.discover( getPlotData(), meta -> meta.getMetadata().getSampleMetadata().getTimeWindow() );
        SortedSet<OneOrTwoThresholds> thresholds =
                Slicer.discover( getPlotData(), meta -> meta.getMetadata().getSampleMetadata().getThresholds() );

        if ( ( timeWindows.size() == 1 ) && ( thresholds.size() == 1 ) )
        {
            return "All Data"; //All of the data is in a single series.
        }
        else if ( ( !timeWindows.isEmpty() ) && ( thresholds.size() == 1 ) )
        {
            return Long.toString( getPlotData().getData()
                                               .get( series )
                                               .getMetadata()
                                               .getSampleMetadata()
                                               .getTimeWindow()
                                               .getLatestLeadTimeInHours() );
        }
        else
        {
            return getPlotData().getData()
                                .get( series )
                                .getMetadata()
                                .getSampleMetadata()
                                .getThresholds()
                                .toStringWithoutUnits();
        }
    }

}
