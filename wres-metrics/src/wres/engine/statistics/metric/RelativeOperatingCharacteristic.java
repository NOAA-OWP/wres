package wres.engine.statistics.metric;

import java.util.Objects;

import wres.datamodel.metric.DataFactory;
import wres.datamodel.metric.DichotomousPairs;
import wres.datamodel.metric.DiscreteProbabilityPairs;
import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.MultiVectorOutput;
import wres.datamodel.metric.ScalarOutput;

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

public final class RelativeOperatingCharacteristic extends Metric<DiscreteProbabilityPairs, MultiVectorOutput>
{

    /**
     * Components of the ROC.
     */

    private final MetricCollection<DichotomousPairs, ScalarOutput> roc;

    @Override
    public MultiVectorOutput apply(final DiscreteProbabilityPairs s)
    {
        Objects.requireNonNull(s, "Specify non-null input for the '" + toString() + "'.");
        //Determine the empirical ROC. 
        //For each classifier, derive the pairs of booleans and compute the PoD and PoFD from the
        //2x2 contingency table, using a metric collection to compute the table only once
        
        
        
        
        

        //Filter by occurrence or non-occurrence

//        Map<Double, List<PairOfDoubles>> filtered =
//                                                  s.getData()
//                                                   .stream()
//                                                   .collect(Collectors.groupingBy(PairOfDoubles::getItemOne));
//        //Sort forecast probabilities 
//        double[] probWhenObsNo = filtered.get(0.0).stream().mapToDouble(PairOfDoubles::getItemTwo).sorted().toArray();
//        Arrays.
//        
//
//        final MetricOutputMetadata metOut = getMetadata(s, s.getData().size(), MetricConstants.MAIN, null);
        return null;
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC;
    }

    @Override
    public boolean hasRealUnits()
    {
        return false;
    }

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    protected static class RelativeOperatingCharacteristicBuilder
    extends
        MetricBuilder<DiscreteProbabilityPairs, MultiVectorOutput>
    {

        @Override
        protected RelativeOperatingCharacteristic build()
        {
            return new RelativeOperatingCharacteristic(this.dataFactory);
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param dataFactory the {@link DataFactory}.
     */

    protected RelativeOperatingCharacteristic(final DataFactory dataFactory)
    {
        super(dataFactory);
        roc = MetricFactory.getInstance(dataFactory)
                           .ofDichotomousScalarCollection(MetricConstants.PROBABILITY_OF_DETECTION,
                                                          MetricConstants.PROBABILITY_OF_FALSE_DETECTION);
    }
}
