package wres.engine.statistics.metric;

import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import org.apache.commons.math3.util.Precision;

import wres.datamodel.PairOfDoubles;
import wres.datamodel.metric.DiscreteProbabilityPairs;
import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.MetricOutputMetadata;
import wres.datamodel.metric.MultiVectorOutput;

/**
 * <p>
 * Computes the Reliability Diagram, which compares the forecast probability of a discrete event against the conditional
 * observed probability, given the forecast probability. Using the sample data, the forecast probabilities are collected
 * into a series of bins that map the unit interval, [0,1]. The corresponding dichotomous observations are pooled and
 * the conditional observed probability is obtained from the average of the pooled observations within each bin. The
 * forecast probability is also obtained from the average of the pooled forecast probabilities within each bin.
 * </p>
 * <p>
 * The Reliability Diagram comprises the average forecast probabilities, the average conditional observed probabilities,
 * and the sample sizes (sharpness) for each bin within the unit interval.
 * </p>
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public final class ReliabilityDiagram extends Metric<DiscreteProbabilityPairs, MultiVectorOutput>
{

    /**
     * Number of bins in the Reliability Diagram.
     */

    private final int bins;

    @Override
    public MultiVectorOutput apply(final DiscreteProbabilityPairs s)
    {
        Objects.requireNonNull(s, "Specify non-null input for the '" + toString() + "'.");
        //Determine the probabilities and sample sizes 
        double constant = 1.0 / bins;
        double[] fProb = new double[bins];
        double[] oProb = new double[bins];
        double[] samples = new double[bins];
        //Consumer that increments the probabilities and sample size
        Consumer<PairOfDoubles> mapper = pair -> {
            //Determine forecast bin
            for(int i = 0; i < bins; i++)
            {
                //Define the bin
                double lower = Precision.round(i * constant, 5);
                double upper = Precision.round(lower + constant, 5);
                if(i == 0)
                {
                    lower = -1.0; //Catch forecast probabilities of zero in the first bin
                }
                //Establish whether the forecast probability falls inside it
                if(pair.getItemTwo() > lower && pair.getItemTwo() <= upper)
                {
                    fProb[i] += pair.getItemTwo();
                    oProb[i] += pair.getItemOne();
                    samples[i] += 1;
                    break;
                }
            }
        };
        //Compute the average probabilities
        s.getData().forEach(mapper);
        for(int i = 0; i < bins; i++)
        {
            fProb[i] = fProb[i] / samples[i];
            oProb[i] = oProb[i] / samples[i];
        }

        //Set the results
        Map<MetricConstants, double[]> output = new EnumMap<>(MetricConstants.class);
        output.put(MetricConstants.FORECAST_PROBABILITY, fProb);
        output.put(MetricConstants.OBSERVED_GIVEN_FORECAST_PROBABILITY, oProb);
        output.put(MetricConstants.SAMPLE_SIZE, samples);
        final MetricOutputMetadata metOut = getMetadata(s, s.getData().size(), MetricConstants.MAIN, null);
        return getDataFactory().ofMultiVectorOutput(output, metOut);
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.RELIABILITY_DIAGRAM;
    }

    @Override
    public boolean hasRealUnits()
    {
        return false;
    }

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    protected static class ReliabilityDiagramBuilder extends MetricBuilder<DiscreteProbabilityPairs, MultiVectorOutput>
    {

        @Override
        protected ReliabilityDiagram build()
        {
            return new ReliabilityDiagram(this);
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     */

    protected ReliabilityDiagram(final ReliabilityDiagramBuilder builder)
    {
        super(builder);
        //Set the default bins
        bins = 10;
    }
}
