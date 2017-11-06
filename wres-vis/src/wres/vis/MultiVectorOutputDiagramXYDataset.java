package wres.vis;

import org.jfree.data.xy.XYDataset;

import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.MultiVectorOutput;

/**
 * The {@link XYDataset} for use in building a chart that plots a {@link MultiVectorOutput}.
 * 
 * @author Hank.Herr
 */
public class MultiVectorOutputDiagramXYDataset extends WRESAbstractXYDataset<MetricOutputMapByTimeAndThreshold<MultiVectorOutput>, MetricOutputMapByTimeAndThreshold<MultiVectorOutput>>
{
    private static final long serialVersionUID = 4254109136599641286L;
    private final MetricDimension xConstant;
    private final MetricDimension yConstant;

    public MultiVectorOutputDiagramXYDataset(final MetricOutputMapByTimeAndThreshold<MultiVectorOutput> input, final MetricDimension xConstant, final MetricDimension yConstant)
    {
        super(input);
        this.xConstant = xConstant;
        this.yConstant = yConstant;
    }

    @Override
    protected void preparePlotData(final MetricOutputMapByTimeAndThreshold<MultiVectorOutput> rawData)
    {
        //This check should not be necessary, since the conditions should be impossible.  I'll do it anyway just to be sure.
        if((rawData.keySetByFirstKey().size() == 0) || (rawData.keySetBySecondKey().size() == 0))
        {
            throw new IllegalStateException("Somehow, one of the key sets, either first or second, is empty.  How did that happen?");
        }
        
        setPlotData(rawData);
    }

    @Override
    public int getItemCount(final int series)
    {
        return getPlotData().get(getPlotData().getKey(series))
                            .getData()
                            .get(xConstant)
                            .getDoubles().length;
    }

    @Override
    public Number getX(final int series, final int item)
    {
        return getPlotData().get(getPlotData().getKey(series))
                            .getData()
                            .get(xConstant)
                            .getDoubles()[item];
    }

    @Override
    public Number getY(final int series, final int item)
    {
        return getPlotData().get(getPlotData().getKey(series))
                            .getData()
                            .get(yConstant)
                            .getDoubles()[item];
    }

    @Override
    public int getSeriesCount()
    {
        return getPlotData().keySet().size();
    }

    @Override
    public Comparable<String> getSeriesKey(final int series)
    {
        if (isLegendNameOverridden(series))
        {
            return getOverrideLegendName(series);
        }
        
        if((getPlotData().keySetByFirstKey().size() == 1) && (getPlotData().keySetBySecondKey().size() == 1))
        {
            return "All Data"; //All of the data is in a single series.
        }
        else if((getPlotData().keySetByFirstKey().size() >= 1) && (getPlotData().keySetBySecondKey().size() == 1))
        {
            return Long.toString( getPlotData().getKey(series).getFirstKey().getLatestLeadTimeInHours() );
        } 
        else
        {
            return getPlotData().getKey(series).getSecondKey().toString();
        } 
    }

}
