package wres.vis;

import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.MultiVectorOutput;

public class RankHistogramXYDataset extends MultiVectorOutputDiagramXYDataset
{
    private static final long serialVersionUID = -798561678672865289L;

    public RankHistogramXYDataset( final MetricOutputMapByTimeAndThreshold<MultiVectorOutput> input,
                                          final MetricDimension xConstant,
                                          final MetricDimension yConstant )
    {
        super( input, xConstant, yConstant );
    }

    @Override
    public Number getEndX( int arg0, int arg1 )
    {
        return getX( arg0, arg1 ).doubleValue() + 0.35;
    }

    @Override
    public Number getStartX( int arg0, int arg1 )
    {
        // TODO Auto-generated method stub
        return getX( arg0, arg1 ).doubleValue() - 0.35;
    }

}
