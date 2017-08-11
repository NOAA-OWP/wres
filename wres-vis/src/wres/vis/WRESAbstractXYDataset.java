package wres.vis;

import java.util.ArrayList;
import java.util.List;

import org.jfree.chart.JFreeChart;
import org.jfree.data.xy.AbstractXYDataset;
import org.jfree.data.xy.XYDataset;

/**
 * Base class for all {@link XYDataset} subclasses implemented for wres-vis. It provides a mechanism for specifying and
 * acquiring default legend names which {@link JFreeChart} acquires via the data set.<br>
 * <br>
 * Override instructions:<br>
 * 1. Override {@link #getPlotData()} setting the return type to the plot data storage container return type for the
 * subclass.<br>
 * <br>
 * 2. Override {@link #preparePlotData(Object)} in order to prepare the plot data. In the simplest case, this should
 * pass through its argument to {@link #setPlotData(Object)}. In more complicated cases, this may need to prepare a data
 * storage container and pass that data storage container to {@link #setPlotData(Object)}.<br>
 * <br>
 * 3. Define the constructor to accept an instance of the appropriate raw input data (metric output data) and pass it to
 * the abstract constructor herein. Perform any actions required afterward, such as, in the case for
 * {@link ScalarOutputByLeadThresholdXYDataset}, setting override legend names.<br>
 * <br>
 * 4. Implement the other {@link AbstractXYDataset} required methods appropriately. Note that the implementation of
 * method {@link #getSeriesKey(int)} should return the value of {@link #getOverrideLegendName(int)} if the method
 * {@link #isLegendNameOverridden(int)} returns true.
 * 
 * @author Hank.Herr
 */
public abstract class WRESAbstractXYDataset extends AbstractXYDataset
{
    /**
     * Generic storage container for the data.
     */
    private Object plotData;

    /**
     * Records overrides for the default legend entries for the different series to plot. This is what allows the
     * {@link ChartEngineFactory} to pass through user overrides of the legend entries.
     */
    private final List<String> overrideLegendNames = new ArrayList<>();

    protected WRESAbstractXYDataset(final Object rawData)
    {
        preparePlotData(rawData);
        for(int i = 0; i < getSeriesCount(); i++)
        {
            overrideLegendNames.add(null);
        }
    }

    protected void setPlotData(final Object plotData)
    {
        this.plotData = plotData;
    }

    protected Object getPlotDataAsObject()
    {
        return plotData;
    }

    /**
     * Called during construction, this must prepare the plot data (from the point of view of the subclass and its
     * corresponding {@link WRESAbstractXYDataset}) from the raw data, stored as an {@link Object}, that is passed into
     * the constructor of this abstract class. It must then call {@link #setPlotData(Object)} so that it is stored
     * herein.
     */
    protected abstract void preparePlotData(Object rawData);

    /**
     * Must be overridden in order to define the return class for this method so that others may call it without
     * casting.
     */
    public abstract Object getPlotData();

    public void setOverrideLegendName(final int seriesIndex, final String name)
    {
        overrideLegendNames.set(seriesIndex, name);
    }

    public String getOverrideLegendName(final int seriesIndex)
    {
        return overrideLegendNames.get(seriesIndex);
    }

    public boolean isLegendNameOverridden(final int seriesIndex)
    {
        return overrideLegendNames.get(seriesIndex) != null;
    }
}
