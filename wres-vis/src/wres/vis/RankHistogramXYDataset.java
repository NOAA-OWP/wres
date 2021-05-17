package wres.vis;

import java.time.temporal.ChronoUnit;
import java.util.List;

import wres.datamodel.metrics.MetricConstants.MetricDimension;
import wres.datamodel.statistics.DiagramStatisticOuter;

class RankHistogramXYDataset extends DiagramStatisticXYDataset
{
    private static final long serialVersionUID = -798561678672865289L;

    /**
     * Build a new rank histogram.
     * 
     * @param input the list of inputs to plot
     * @param xConstant the dimension for the domain axis
     * @param yConstant the dimension for the range axis
     * @param durationUnits the duration units
     * @throws NullPointerException if any input is null
     */

    RankHistogramXYDataset( final List<DiagramStatisticOuter> input,
                            final MetricDimension xConstant,
                            final MetricDimension yConstant,
                            final ChronoUnit durationUnits )
    {
        super( input, xConstant, yConstant, durationUnits );
    }

    @Override
    public Number getEndX( int arg0, int arg1 )
    {
        return getX( arg0, arg1 ).doubleValue() + 0.35;
    }

    @Override
    public Number getStartX( int arg0, int arg1 )
    {
        return getX( arg0, arg1 ).doubleValue() - 0.35;
    }

}
