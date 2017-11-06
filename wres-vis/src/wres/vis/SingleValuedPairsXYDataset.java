package wres.vis;

import org.jfree.data.xy.AbstractXYDataset;

import wres.datamodel.inputs.pairs.SingleValuedPairs;

/**
 * An {@link AbstractXYDataset} that wraps a {@link SingleValuedPairs}
 * 
 * @author Hank.Herr
 */
//TODO Note that this needs further work whenever we let wres-vis build scatter plots for display.
//Specifically, we need to think about what the legend entry should look like (see below for a first attempt)
//as well as how to handle multiple sereis being plotted (i.e., how to store the data.).
public class SingleValuedPairsXYDataset extends WRESAbstractXYDataset<SingleValuedPairs, SingleValuedPairs> //implements DomainInfo, XisSymbolic, RangeInfo
{
    public SingleValuedPairsXYDataset(final SingleValuedPairs input)
    {
        super(input);
    }

    @Override
    protected void preparePlotData(final SingleValuedPairs rawData)
    {
        setPlotData(rawData);
    }

    @Override
    public int getItemCount(final int series)
    {
        return getPlotData().getData().size();
    }

    @Override
    public Number getX(final int series, final int item)
    {
        return getPlotData().getData().get(item).getItemOne();
    }

    @Override
    public Number getY(final int series, final int item)
    {
        return getPlotData().getData().get(item).getItemTwo();
    }

    @Override
    public int getSeriesCount()
    {
        return 1;
    }

    @Override
    public Comparable<String> getSeriesKey(final int series)
    {
        if(isLegendNameOverridden(series))
        {
            return getOverrideLegendName(series);
        }
        return getPlotData().getMetadata().getIdentifier().getGeospatialID() + "."
            + getPlotData().getMetadata().getIdentifier().getVariableID() + "."
            + getPlotData().getMetadata().getIdentifier().getScenarioID();
    }
}
