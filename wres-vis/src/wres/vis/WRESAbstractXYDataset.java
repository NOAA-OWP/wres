package wres.vis;

import java.util.ArrayList;
import java.util.List;

import org.jfree.chart.JFreeChart;
import org.jfree.data.xy.AbstractIntervalXYDataset;
import org.jfree.data.xy.AbstractXYDataset;
import org.jfree.data.xy.XYDataset;

/**
 * Base class for all {@link XYDataset} subclasses implemented for wres-vis. It provides a mechanism for specifying and
 * acquiring default legend names which {@link JFreeChart} acquires via the data set.<br>
 * <br>
 * Override instructions:<br>
 * 1. Define the generic types appropriately; see parameter descriptions.<br>
 * <br>
 * 2. Override {@link #preparePlotData(Object)} in order to prepare the plot data. In the simplest case, this should
 * pass through its argument to {@link #setPlotData(Object)}. In more complicated cases, this may need to prepare a data
 * storage container and pass that data storage container to {@link #setPlotData(Object)}. Be sure to call
 * {@link #setPlotData(Object)}.<br>
 * <br>
 * 3. Define the constructor to accept an instance of the appropriate raw input data (generic type U) and pass it to the
 * abstract constructor herein. Perform any actions required afterward, such as, in the case for
 * {@link ScalarOutputByLeadAndThresholdXYDataset}, setting override legend names.<br>
 * <br>
 * 4. Implement the other {@link AbstractXYDataset} required methods appropriately. Note that the implementation of
 * method {@link #getSeriesKey(int)} should return the value of {@link #getOverrideLegendName(int)} if the method
 * {@link #isLegendNameOverridden(int)} returns true.
 * 
 * @author Hank.Herr
 * @param <T> The data type of the plot data which is stored internally within {@link #plotData}, referred to vie
 *            {@link #getPlotData()}, and referred to as needed in subclasses.
 * @param <U> The raw data from which the plot data will be prepared and which is passed into the constructor method,
 *            {@link WRESAbstractXYDataset#WRESAbstractXYDataset(Object)}. Most often, this will be identical to the
 *            plot data generic type.
 */
@SuppressWarnings( "serial" )
public abstract class WRESAbstractXYDataset<T, U> extends AbstractIntervalXYDataset
{
    /**
     * Generic storage container for the data to plot which is derived from the raw data passed into constructor.
     */
    private transient T plotData;

    /**
     * Records overrides for the default legend entries for the different series to plot. This is what allows the
     * {@link ChartEngineFactory} to pass through user overrides of the legend entries.
     */
    private final List<String> overrideLegendNames = new ArrayList<>();

    protected WRESAbstractXYDataset(final U rawData)
    {
        preparePlotData(rawData);
        for(int i = 0; i < getSeriesCount(); i++)
        {
            overrideLegendNames.add(null);
        }
    }

    protected void setPlotData(final T plotData)
    {
        this.plotData = plotData;
    }

    public T getPlotData()
    {
        return plotData;
    }

    /**
     * Called during construction, this must prepare the plot data (from the point of view of the subclass and its
     * corresponding {@link WRESAbstractXYDataset}) from the raw data provided (the same data passed into the
     * constructor {@link WRESAbstractXYDataset#WRESAbstractXYDataset(Object)}). It must then call
     * {@link #setPlotData(Object)} so that it is stored herein.
     * @param rawData the raw data 
     */
    protected abstract void preparePlotData(U rawData);

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
    
    @Override
    public Number getEndX( int arg0, int arg1 )
    {
        return getX(arg0, arg1);
    }

    @Override
    public Number getEndY( int arg0, int arg1 )
    {
        return getY(arg0, arg1);
    }

    @Override
    public Number getStartX( int arg0, int arg1 )
    {
        return getX(arg0, arg1);
    }

    @Override
    public Number getStartY( int arg0, int arg1 )
    {
        return getY(arg0, arg1);
    }
}
