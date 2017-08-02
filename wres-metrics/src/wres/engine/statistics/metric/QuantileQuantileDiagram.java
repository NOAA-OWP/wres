package wres.engine.statistics.metric;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;

import wres.datamodel.metric.DataFactory;
import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.MetricOutputMetadata;
import wres.datamodel.metric.MultiVectorOutput;
import wres.datamodel.metric.SingleValuedPairs;
import wres.datamodel.metric.Slicer;

/**
 * Compares the quantiles of two samples at a prescribed number (<code>N</code>) of (evenly-spaced) probabilities on the
 * unit interval, namely <code>{1/N+1,...,N/N+1}</code>. If the samples originate from the same probability
 * distribution, the order statistics (and hence the quantiles) should be the same, notwithstanding any sampling error.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public final class QuantileQuantileDiagram extends Metric<SingleValuedPairs, MultiVectorOutput>
{

    /**
     * The number of probabilities at which to compute the order statistics.
     */

    private final int probCount;

    @Override
    public MultiVectorOutput apply(SingleValuedPairs s)
    {
        Objects.requireNonNull(s, "Specify non-null input for the '" + toString() + "'.");
        DataFactory d = getDataFactory();
        Slicer slicer = d.getSlicer();
        //Determine the number of order statistics to compute
        double[] observedQ = new double[probCount];
        double[] predictedQ = new double[probCount];

        //Get the ordered data
        double[] sortedLeft = slicer.getLeftSide(s);
        double[] sortedRight = slicer.getRightSide(s);
        Arrays.sort(sortedLeft);
        Arrays.sort(sortedRight);

        //Compute the order statistics
        for(int i = 0; i < probCount; i++)
        {
            double prob = (i+1.0) / (probCount+1.0);
            observedQ[i] = slicer.getQuantile(prob, sortedLeft);
            predictedQ[i] = slicer.getQuantile(prob, sortedRight);
        }

        //Set and return the results
        Map<MetricConstants, double[]> output = new EnumMap<>(MetricConstants.class);
        output.put(MetricConstants.OBSERVED_QUANTILES, observedQ);
        output.put(MetricConstants.PREDICTED_QUANTILES, predictedQ);
        final MetricOutputMetadata metOut = getMetadata(s, s.getData().size(), MetricConstants.MAIN, null);
        return d.ofMultiVectorOutput(output, metOut);
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

    protected static class QuantileQuantileDiagramBuilder extends MetricBuilder<SingleValuedPairs, MultiVectorOutput>
    {

        @Override
        protected QuantileQuantileDiagram build()
        {
            return new QuantileQuantileDiagram(this.dataFactory);
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param dataFactory the {@link DataFactory}.
     */

    private QuantileQuantileDiagram(final DataFactory dataFactory)
    {
        super(dataFactory);
        //Set the number of thresholds to 1000
        probCount = 1000;
    }
}
