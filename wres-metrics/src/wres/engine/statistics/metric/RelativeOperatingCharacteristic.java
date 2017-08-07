package wres.engine.statistics.metric;

import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.math3.util.Precision;

import wres.datamodel.metric.DataFactory;
import wres.datamodel.metric.DichotomousPairs;
import wres.datamodel.metric.DiscreteProbabilityPairs;
import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.MetricOutput;
import wres.datamodel.metric.MetricOutputMapByMetric;
import wres.datamodel.metric.MetricOutputMetadata;
import wres.datamodel.metric.MultiVectorOutput;
import wres.datamodel.metric.ScalarOutput;
import wres.datamodel.metric.Slicer;

/**
 * Computes the Relative Operating Characteristic (ROC; also known as the Receiver Operating Characteristic), which
 * compares the probability of detection (PoFD) against the probability of false detection (PoFD). The empirical ROC is
 * computed for a discrete number of probability thresholds or classifiers that determine whether the forecast event
 * occurred, based on the forecast probability.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public abstract class RelativeOperatingCharacteristic<T extends MetricOutput<?>>
extends
    Metric<DiscreteProbabilityPairs, T>
{

    /**
     * Components of the ROC.
     */

    private final MetricCollection<DichotomousPairs, ScalarOutput> roc;

    /**
     * Returns the components of the Relative Operating Characteristic for a prescribed number of thresholds. The
     * thresholds are used to divide the unit interval equally. A binary classifier is derived from each threshold and
     * used to classify the observed and forecast probabilities of a discrete event according to whether the threshold
     * is exceeded. Each classifier produces a pair of MetricConstants.PROBABILITY_OF_DETECTION (PoD) and
     * MetricConstants.PROBABILITY_OF_FALSE_DETECTION (PoFD), which are returned in the result.
     * 
     * @param s the pairs
     * @param points the number of thresholds
     * @return a {@link MultiVectorOutput} containing the pairs of PoD and PoFD
     */

    MultiVectorOutput getROC(final DiscreteProbabilityPairs s, int points)
    {
        Objects.requireNonNull(s, "Specify non-null input for the '" + toString() + "'.");
        //Determine the empirical ROC. 
        //For each classifier, derive the pairs of booleans and compute the PoD and PoFD from the
        //2x2 contingency table, using a metric collection to compute the table only once
        double constant = 1.0 / points;
        double[] pOD = new double[points + 1];
        double[] pOFD = new double[points + 1];
        DataFactory d = getDataFactory();
        Slicer slice = d.getSlicer();

        for(int i = 1; i < points; i++)
        {
            double prob = Precision.round(1.0 - (i * constant), 5);
            //Compute the PoD/PoFD using the probability threshold to determine whether the event occurred
            //according to the probability on the RHS
            MetricOutputMapByMetric<ScalarOutput> out =
                                                      roc.apply(slice.transformPairs(s,
                                                                                     in -> d.pairOf(Double.compare(in.getItemOne(),
                                                                                                                   1.0) == 0,
                                                                                                    in.getItemTwo() > prob)));
            //Store
            pOD[i] = out.get(MetricConstants.PROBABILITY_OF_DETECTION).getData();
            pOFD[i] = out.get(MetricConstants.PROBABILITY_OF_FALSE_DETECTION).getData();
        }
        //Set the upper point to (1.0, 1.0)
        pOD[points] = 1.0;
        pOFD[points] = 1.0;

        //Set the results
        Map<MetricConstants, double[]> output = new EnumMap<>(MetricConstants.class);
        output.put(MetricConstants.PROBABILITY_OF_DETECTION, pOD);
        output.put(MetricConstants.PROBABILITY_OF_FALSE_DETECTION, pOFD);
        final MetricOutputMetadata metOut = getMetadata(s, s.getData().size(), MetricConstants.MAIN, null);
        return d.ofMultiVectorOutput(output, metOut);
    }

    @Override
    public boolean hasRealUnits()
    {
        return false;
    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     */

    RelativeOperatingCharacteristic(final MetricBuilder<DiscreteProbabilityPairs, T> builder)
    {
        super(builder);
        roc = MetricFactory.getInstance(builder.dataFactory)
                           .ofDichotomousScalarCollection(MetricConstants.PROBABILITY_OF_DETECTION,
                                                          MetricConstants.PROBABILITY_OF_FALSE_DETECTION);
    }
}
