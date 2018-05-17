package wres.engine.statistics.metric.singlevalued;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.DoubleUnaryOperator;

import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.Slicer;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.MultiVectorOutput;
import wres.engine.statistics.metric.Diagram;
import wres.engine.statistics.metric.MetricParameterException;

/**
 * Compares the quantiles of two samples at a prescribed number (<code>N</code>) of (evenly-spaced) probabilities on the
 * unit interval, namely <code>{1/N+1,...,N/N+1}</code>. If the samples originate from the same probability
 * distribution, the order statistics (and hence the quantiles) should be the same, notwithstanding any sampling error.
 * 
 * @author james.brown@hydrosolved.com
 */

public class QuantileQuantileDiagram extends Diagram<SingleValuedPairs, MultiVectorOutput>
{

    /**
     * The number of probabilities at which to compute the order statistics.
     */

    private final int probCount;

    @Override
    public MultiVectorOutput apply( SingleValuedPairs s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new MetricInputException( "Specify non-null input to the '" + this + "'." );
        }
        DataFactory d = getDataFactory();
        Slicer slicer = d.getSlicer();
        //Determine the number of order statistics to compute
        double[] observedQ = new double[probCount];
        double[] predictedQ = new double[probCount];

        //Get the ordered data
        double[] sortedLeft = slicer.getLeftSide( s );
        double[] sortedRight = slicer.getRightSide( s );
        Arrays.sort( sortedLeft );
        Arrays.sort( sortedRight );
        DoubleUnaryOperator qLeft = slicer.getQuantileFunction( sortedLeft );
        DoubleUnaryOperator qRight = slicer.getQuantileFunction( sortedRight );

        //Compute the order statistics
        for ( int i = 0; i < probCount; i++ )
        {
            double prob = ( i + 1.0 ) / ( probCount + 1.0 );
            observedQ[i] = qLeft.applyAsDouble( prob );
            predictedQ[i] = qRight.applyAsDouble( prob );
        }

        //Set and return the results
        Map<MetricDimension, double[]> output = new EnumMap<>( MetricDimension.class );
        output.put( MetricDimension.OBSERVED_QUANTILES, observedQ );
        output.put( MetricDimension.PREDICTED_QUANTILES, predictedQ );
        final MetricOutputMetadata metOut = getMetadata( s, s.getRawData().size(), MetricConstants.MAIN, null );
        return d.ofMultiVectorOutput( output, metOut );
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.QUANTILE_QUANTILE_DIAGRAM;
    }

    @Override
    public boolean hasRealUnits()
    {
        return true;
    }

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    public static class QuantileQuantileDiagramBuilder extends DiagramBuilder<SingleValuedPairs, MultiVectorOutput>
    {

        @Override
        public QuantileQuantileDiagram build() throws MetricParameterException
        {
            return new QuantileQuantileDiagram( this );
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     * @throws MetricParameterException if one or more parameters is invalid 
     */

    private QuantileQuantileDiagram( final QuantileQuantileDiagramBuilder builder ) throws MetricParameterException
    {
        super( builder );
        //Set the number of thresholds to 1000
        probCount = 1000;
    }
}
