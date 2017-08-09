package wres.datamodel.metric;

import java.util.Objects;

/**
 * Concrete implementation of a {@link Threshold}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
class SafeThreshold implements Threshold
{

    /**
     * The threshold value
     */

    protected final Double threshold;

    /**
     * The upper threshold value or null.
     */

    protected final Double thresholdUpper;

    /**
     * The threshold condition.
     */

    protected final Operator condition;

    /**
     * Construct the threshold.
     *
     * @param threshold the threshold
     * @param thresholdUpper the upper threshold or null
     * @param condition the condition
     */

    protected SafeThreshold(final Double threshold, final Double thresholdUpper, final Operator condition)
    {
        //Bounds checks
        Objects.requireNonNull(threshold, "Specify a non-null threshold.");
        Objects.requireNonNull(condition, "Specify a non-null condition.");
        if(condition.equals(Operator.BETWEEN))
        {
            Objects.requireNonNull(thresholdUpper, "Specify a non-null upper threshold.");
            if(thresholdUpper <= threshold)
            {
                throw new IllegalArgumentException("The upper threshold must be greater than the lower threshold: ["
                    + threshold + "," + thresholdUpper + "].");
            }
        }
        else
        {
            if(Objects.nonNull(thresholdUpper))
            {
                throw new IllegalArgumentException("Specify a null upper threshold or define an "
                    + "appropriate BETWEEN condition.");
            }
        }
        this.threshold = threshold;
        this.thresholdUpper = thresholdUpper;
        this.condition = condition;
    }

    @Override
    public Double getThreshold()
    {
        return threshold;
    }

    @Override
    public Operator getCondition()
    {
        return condition;
    }

    @Override
    public Double getThresholdUpper()
    {
        return thresholdUpper;
    }

    @Override
    public boolean hasBetweenCondition()
    {
        return condition.equals(Operator.BETWEEN);
    }

    @Override
    public boolean equals(final Object o)
    {
        if(!(o instanceof SafeThreshold))
        {
            return false;
        }
        final SafeThreshold in = (SafeThreshold)o;
        boolean returnMe = in.getThreshold().equals(threshold) && in.getCondition().equals(condition);
        if(in.hasBetweenCondition())
        {
            returnMe = returnMe && thresholdUpper.equals(in.getThresholdUpper());
        }
        return returnMe;
    }

    @Override
    public int hashCode()
    {
        int returnMe = threshold.hashCode() + condition.hashCode();
        if(hasBetweenCondition())
        {
            returnMe += thresholdUpper.hashCode();
        }
        return returnMe;
    }

    @Override
    public String toString()
    {
        if(threshold.equals(Double.NEGATIVE_INFINITY))
        {
            return "All data";
        }
        final String c = getConditionID();
        if(hasBetweenCondition())
        {
            return ">= " + threshold + " && < " + thresholdUpper;
        }
        return c + threshold;
    }

    /**
     * Returns a string representation of the elementary condition (e.g. one part of a {@link Operator#BETWEEN}
     * condition).
     * 
     * @return a string for the elementary condition
     */

    protected String getConditionID()
    {
        switch(condition)
        {
            case GREATER:
                return "> ";
            case LESS:
                return "< ";
            case GREATER_EQUAL:
                return ">= ";
            case LESS_EQUAL:
                return "<= ";
            case EQUAL:
                return "= ";
            default:
                return null;
        }
    }

    @Override
    public int compareTo(final Threshold o)
    {
        Objects.requireNonNull(o, "Specify a non-null threshold for comparison");
        int returnMe = o.getCondition().compareTo(condition);
        if(returnMe != 0)
        {
            return returnMe;
        }
        returnMe = Double.compare(o.getThreshold(), threshold);
        if(returnMe != 0)
        {
            return returnMe;
        }
        if(hasBetweenCondition())
        {
            returnMe = Double.compare(o.getThresholdUpper(), thresholdUpper);
            if(returnMe != 0)
            {
                return returnMe;
            }
        }
        return 0;
    }

    @Override
    public boolean test(Double t)
    {
        switch(condition)
        {
            case GREATER:
                return t > threshold;
            case LESS:
                return t < threshold;
            case GREATER_EQUAL:
                return t >= threshold;
            case LESS_EQUAL:
                return t <= threshold;
            case BETWEEN:
                return t >= threshold && t < thresholdUpper;
            case EQUAL:
                return Double.compare(t, threshold) == 0;
            default:
                throw new UnsupportedOperationException("Unexpected logical condition.");
        }
    }

    @Override
    public boolean isFinite()
    {
        boolean returnMe = Double.isFinite(threshold);
        if(hasBetweenCondition()) {
            returnMe = returnMe && Double.isFinite(thresholdUpper);
        }
        return returnMe;
    }

}
