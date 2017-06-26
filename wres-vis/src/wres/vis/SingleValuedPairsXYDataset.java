package wres.vis;

import java.util.ArrayList;
import java.util.List;

import org.jfree.data.xy.AbstractXYDataset;

import wres.engine.statistics.metric.inputs.SingleValuedPairs;

/**
 * An {@link AbstractXYDataset} that wraps a {@link SingleValuedPairs}
 * 
 * @author Hank.Herr
 */
//TODO This needs to extend a super class (extension of AbstractXYDataset) that contains the legend name
//     methods!!!  No reason for those to be in here when ALL instances of AbstractXYDataset will need it.
//TODO A question to answer: Would it be better for this to be returned by SingleValuedPairs, which is 
//     a MetricInput.  In other words, should MetricInput include a method "XYDataset buildXYDataset()"?
public class SingleValuedPairsXYDataset extends AbstractXYDataset //implements DomainInfo, XisSymbolic, RangeInfo
{
    private final SingleValuedPairs pairs;
    private final List<String> legendNames = new ArrayList<>();

    public SingleValuedPairsXYDataset(final SingleValuedPairs input)
    {
        pairs = input;

        //Default legend names.
        for(int series = 0; series < pairs.size(); series++)
        {
            legendNames.add("Series " + series);
        }
    }

    public void setLegendName(final int series, final String name)
    {
        legendNames.set(series, name);
    }

    @Override
    public int getItemCount(final int series)
    {
        return pairs.getData(series).size();
    }

    @Override
    public Number getX(final int series, final int item)
    {
        return pairs.getData(series).get(item).getItemOne();
    }

    @Override
    public Number getY(final int series, final int item)
    {
        return pairs.getData(series).get(item).getItemTwo();
    }

    @Override
    public int getSeriesCount()
    {
        return pairs.size();
    }

    @Override
    public Comparable<String> getSeriesKey(final int series)
    {
        return legendNames.get(series);
    }
}
