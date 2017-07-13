package wres.datamodel.metric;

import java.util.Objects;

/**
 * Concrete implementation of a {@link Quantile}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

final class SafeQuantileKey extends SafeThresholdKey implements Quantile
{

    /**
     * The probability associated with the quantile.
     */

    private final Double probability;

    /**
     * The probabiity associated with the upper quantile or null.
     */

    private final Double probabilityUpper;

    /**
     * Construct the quantile.
     *
     * @param threshold the threshold
     * @param thresholdUpper the upper threshold or null
     * @param probability the probability within [0,1] or {@link Double#NEGATIVE_INFINITY}
     * @param probabilityUpper the probability for the upper threshold within [0,1] or {@link Double#NEGATIVE_INFINITY}
     *            or null
     * @param condition the condition
     */

    protected SafeQuantileKey(final Double threshold,
                          final Double thresholdUpper,
                          final Double probability,
                          final Double probabilityUpper,
                          final Condition condition)
    {
        super(threshold, thresholdUpper, condition);
        //Bounds checks
        Objects.requireNonNull(probability, "Specify a non-null probability for the map key.");
        //Negative infinity allowed for all data 
        if(!probability.equals(Double.NEGATIVE_INFINITY) && (probability < 0.0 || probability > 1.0))
        {
            throw new IllegalArgumentException("The threshold probability is out of bounds [0,1]: " + probability);
        }
        if(condition.equals(Condition.BETWEEN))
        {
            Objects.requireNonNull(probabilityUpper, "Specify a non-null upper threshold probability for the map key.");
            if(!probability.equals(Double.NEGATIVE_INFINITY) && (probabilityUpper < 0.0 || probabilityUpper > 1.0))
            {
                throw new IllegalArgumentException("The threshold probability is out of bounds [0,1]: "
                    + probabilityUpper);
            }
            if(probabilityUpper <= probability)
            {
                throw new IllegalArgumentException("The upper threshold probability must be greater than the "
                    + "lower threshold probability: [" + probability + "," + probabilityUpper + "].");
            }
        }
        else
        {
            if(!Objects.isNull(probabilityUpper))
            {
                throw new IllegalArgumentException("Specify a null upper threshold probability for the map key "
                    + "or define an appropriate BETWEEN condition.");
            }
        }
        this.probability = probability;
        this.probabilityUpper = probabilityUpper;
    }

    @Override
    public Double getThresholdProbability()
    {
        return probability;
    }

    @Override
    public Double getThresholdUpperProbability()
    {
        return probabilityUpper;
    }

    @Override
    public boolean equals(final Object o)
    {
        boolean returnMe = super.equals(o) && o instanceof SafeQuantileKey;
        if(returnMe)
        {
            final SafeQuantileKey in = (SafeQuantileKey)o;
            returnMe = returnMe && in.getThresholdProbability().equals(probability);
            if(in.hasBetweenCondition())
            {
                returnMe = returnMe && probabilityUpper.equals(in.getThresholdUpperProbability());
            }
        }
        return returnMe;
    }

    @Override
    public int compareTo(final Threshold o)
    {
        int returnMe = super.compareTo(o);
        if(returnMe != 0)
        {
            return returnMe;
        }
        //Check additional quantile fields
        if(o instanceof Quantile)
        {
            final Quantile q = (Quantile)o;
            returnMe = Double.compare(probability,q.getThresholdProbability());
            if(returnMe != 0)
            {
                return returnMe;
            }
            if(hasBetweenCondition())
            {
                returnMe = Double.compare(probabilityUpper,q.getThresholdUpperProbability());
                if(returnMe != 0)
                {
                    return returnMe;
                }
            }
        }
        return 0;
    }

    @Override
    public int hashCode()
    {
        int returnMe = super.hashCode() + probability.hashCode();
        if(hasBetweenCondition())
        {
            returnMe += probabilityUpper.hashCode();
        }
        return returnMe;
    }

    @Override
    public String toString()
    {
        if(threshold.equals(Double.NEGATIVE_INFINITY)) {
            return super.toString();
        }
        final String c = getConditionID();
        String common = " [Pr = ";
        if(hasBetweenCondition())
        {
            return ">= " + threshold + common + probability + "] && < " + thresholdUpper + common
                + probabilityUpper + "]";
        }
        return c + threshold + common + probability + "]";
    }

}
