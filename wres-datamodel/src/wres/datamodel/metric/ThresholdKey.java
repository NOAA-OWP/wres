package wres.datamodel.metric;

import java.util.Objects;

/**
 * Concrete implementation of a {@link Threshold}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
class ThresholdKey implements Threshold
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
    
    protected final Condition condition;

    /**
     * Construct the threshold.
     *
     * @param threshold the threshold
     * @param thresholdUpper the upper threshold or null
     * @param condition the condition
     */

    protected ThresholdKey(final Double threshold, final Double thresholdUpper, final Condition condition)
    {
        //Bounds checks
        Objects.requireNonNull(threshold, "Specify a non-null threshold for the map key.");
        Objects.requireNonNull(condition, "Specify a non-null condition for the map key.");
        if(condition.equals(Condition.BETWEEN))
        {
            Objects.requireNonNull(thresholdUpper, "Specify a non-null upper threshold for the map key.");
            if(thresholdUpper <= threshold)
            {
                throw new IllegalArgumentException("The upper threshold must be greater than the lower threshold: ["
                    + threshold + "," + thresholdUpper + "].");
            }
        }
        else
        {
            if(!Objects.isNull(thresholdUpper))
            {
                throw new IllegalArgumentException("Specify a null upper threshold for the map key or define an "
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
    public Condition getCondition()
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
        return condition.equals(Condition.BETWEEN);
    }

    @Override
    public boolean equals(final Object o)
    {
        boolean returnMe = o instanceof ThresholdKey;
        if(returnMe)
        {
            final ThresholdKey in = (ThresholdKey)o;
            returnMe = returnMe && in.getThreshold().equals(threshold) && in.getCondition().equals(condition);
            if(in.hasBetweenCondition())
            {
                returnMe = returnMe && thresholdUpper.equals(in.getThresholdUpper());
            }
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
        if(threshold.equals(Double.NEGATIVE_INFINITY)) {
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
     * Returns a string representation of the condition.
     * 
     * @return a string for the condition
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
        if(o.getCondition() != condition)
        {
            return -1;
        }
        int returnMe = Double.compare(threshold,o.getThreshold());
        if(returnMe != 0) {
            return returnMe;
        }
        if(hasBetweenCondition())
        {
            returnMe = Double.compare(thresholdUpper,o.getThresholdUpper());
            if(returnMe != 0) {
                return returnMe;
            }
        }
        return 0;
    }

}
