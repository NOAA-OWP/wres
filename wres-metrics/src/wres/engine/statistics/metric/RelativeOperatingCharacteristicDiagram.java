package wres.engine.statistics.metric;

import wres.datamodel.DiscreteProbabilityPairs;
import wres.datamodel.MetricConstants;
import wres.datamodel.MultiVectorOutput;

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

class RelativeOperatingCharacteristicDiagram extends RelativeOperatingCharacteristic<MultiVectorOutput>
{

    /**
     * Number of points in the empirical ROC diagram.
     */

    private final int points;

    @Override
    public MultiVectorOutput apply(final DiscreteProbabilityPairs s)
    {
        return getROC(s, points);
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM;
    }

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    static class RelativeOperatingCharacteristicBuilder
    extends
        MetricBuilder<DiscreteProbabilityPairs, MultiVectorOutput>
    {

        @Override
        protected RelativeOperatingCharacteristicDiagram build()
        {
            return new RelativeOperatingCharacteristicDiagram(this);
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     */

    private RelativeOperatingCharacteristicDiagram(final RelativeOperatingCharacteristicBuilder builder)
    {
        super(builder);
        //Set the default points
        points = 10;
    }
}
