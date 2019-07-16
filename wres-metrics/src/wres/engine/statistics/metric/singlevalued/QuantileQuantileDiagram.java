package wres.engine.statistics.metric.singlevalued;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.DoubleUnaryOperator;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.Slicer;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.pairs.SingleValuedPairs;
import wres.datamodel.statistics.DiagramStatistic;
import wres.datamodel.statistics.StatisticMetadata;
import wres.engine.statistics.metric.Diagram;

/**
 * Compares the quantiles of two samples at a prescribed number (<code>N</code>) of (evenly-spaced) probabilities on the
 * unit interval, namely <code>{1/N+1,...,N/N+1}</code>. If the samples originate from the same probability
 * distribution, the order statistics (and hence the quantiles) should be the same, notwithstanding any sampling error.
 * 
 * @author james.brown@hydrosolved.com
 */

public class QuantileQuantileDiagram extends Diagram<SingleValuedPairs, DiagramStatistic>
{

    /**
     * The default number of probabilities at which to compute the order statistics.
     */

    private static final int DEFAULT_PROBABILITY_COUNT = 1000;

    /**
     * The number of probabilities at which to compute the order statistics.
     */

    private final int probCount;

    /**
     * Returns an instance.
     * 
     * @return an instance
     */

    public static QuantileQuantileDiagram of()
    {
        return new QuantileQuantileDiagram();
    }

    @Override
    public DiagramStatistic apply( SingleValuedPairs s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new SampleDataException( "Specify non-null input to the '" + this + "'." );
        }

        //Determine the number of order statistics to compute
        double[] observedQ = new double[probCount];
        double[] predictedQ = new double[probCount];

        //Get the ordered data
        double[] sortedLeft = Slicer.getLeftSide( s );
        double[] sortedRight = Slicer.getRightSide( s );
        Arrays.sort( sortedLeft );
        Arrays.sort( sortedRight );
        DoubleUnaryOperator qLeft = Slicer.getQuantileFunction( sortedLeft );
        DoubleUnaryOperator qRight = Slicer.getQuantileFunction( sortedRight );

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
        final StatisticMetadata metOut =
                StatisticMetadata.of( s.getMetadata(),
                                    this.getID(),
                                    MetricConstants.MAIN,
                                    this.hasRealUnits(),
                                    s.getRawData().size(),
                                    null );
        return DiagramStatistic.ofDiagramStatistic( output, metOut );
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
     * Hidden constructor.
     */

    private QuantileQuantileDiagram()
    {
        super();

        this.probCount = DEFAULT_PROBABILITY_COUNT;
    }

}
