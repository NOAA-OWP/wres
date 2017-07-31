package wres.vis;

import java.util.ArrayList;
import java.util.List;

import org.jfree.data.xy.AbstractXYDataset;

import wres.datamodel.metric.MetricOutputMapByLeadThreshold;
import wres.datamodel.metric.ScalarOutput;

/**
 * An {@link AbstractXYDataset} that wraps a {@link MetricOutputMapByLeadThreshold} which contains a set of
 * {@link ScalarOutput} for a single verification metric, indexed by forecast lead time and threshold. Slices the
 * data by lead time to form plots by threshold on the domain axis.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public class ScalarOutputByThresholdLeadXYDataset extends AbstractXYDataset
{

    /**
     * Serial identifier.
     */
    
    private static final long serialVersionUID = 1598160458133121056L;

    /**
     * Legend items.
     */

    private final List<String> legendNames = new ArrayList<>();

    /**
     * Data sliced by series, i.e. one lead time per slice, where each slice contains all thresholds for one score.
     */

    private final transient List<MetricOutputMapByLeadThreshold<ScalarOutput>> data = new ArrayList<>();

    /**
     * Construct.
     * 
     * @param input the input data containing multiple thresholds and lead times
     */

    public ScalarOutputByThresholdLeadXYDataset(final MetricOutputMapByLeadThreshold<ScalarOutput> input)
    {
        //Slice the input data by threshold and store locally
        input.keySetByLead().forEach(key -> {
            data.add(input.sliceByLead(key));
            legendNames.add(key.toString()+"h"); //Assumes WRES uses hours for lead times globally
        });
    }

    /**
     * Set the legend name.
     * 
     * @param series the series index
     * @param name the name
     */
    
    public void setLegendName(final int series, final String name)
    {
        legendNames.set(series, name);
    }

    @Override
    public int getItemCount(final int series)
    {
        return data.get(series).size();
    }

    @Override
    public Number getX(final int series, final int item)
    {
        //Cannot allow all data (infinite) threshold. Use lower bound if this is a "BETWEEN" threshold
        final double test = data.get(series).getKey(item).getSecondKey().getThreshold();
        if(Double.isInfinite(test)) {
            return Double.MIN_VALUE; //JFreeChart missing protocol is to return finite double for X and null for Y
        }
        return test;
    }

    @Override
    public Number getY(final int series, final int item)
    {
        //Cannot allow all data (infinite) threshold
        final Double test = (Double)getX(series,item);
        if(test.equals(Double.MIN_VALUE)) {
            return null;
        }
        return data.get(series).getValue(item).getData();
    }

    @Override
    public int getSeriesCount()
    {
        return data.size();
    }

    @Override
    public Comparable<String> getSeriesKey(final int series)
    {
        return legendNames.get(series);
    }
}
