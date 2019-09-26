package wres.vis;

import org.apache.commons.lang3.tuple.Pair;
import org.jfree.data.xy.AbstractXYDataset;

import wres.datamodel.sampledata.SampleData;

/**
 * An {@link AbstractXYDataset} for single-valued pairs.
 * 
 * @author Hank.Herr
 */
//TODO Note that this needs further work whenever we let wres-vis build scatter plots for display.
//Specifically, we need to think about what the legend entry should look like (see below for a first attempt)
//as well as how to handle multiple sereis being plotted (i.e., how to store the data.).
@SuppressWarnings( "serial" )
public class SingleValuedPairsXYDataset
        extends WRESAbstractXYDataset<SampleData<Pair<Double, Double>>, SampleData<Pair<Double, Double>>> //implements DomainInfo, XisSymbolic, RangeInfo
{
    public SingleValuedPairsXYDataset(final SampleData<Pair<Double,Double>> input)
    {
        super(input);
    }

    @Override
    protected void preparePlotData(final SampleData<Pair<Double,Double>> rawData)
    {
        setPlotData(rawData);
    }

    @Override
    public int getItemCount(final int series)
    {
        return getPlotData().getRawData().size();
    }

    @Override
    public Number getX(final int series, final int item)
    {
        return getPlotData().getRawData().get(item).getLeft();
    }

    @Override
    public Number getY(final int series, final int item)
    {
        return getPlotData().getRawData().get(item).getRight();
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
