package wres.engine.statistics.metric;

import wres.datamodel.metric.EnsemblePairs;
import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.MetricConstants.MetricDecompositionGroup;
import wres.datamodel.metric.MetricOutputMetadata;
import wres.datamodel.metric.VectorOutput;

/**
 * <p>
 * The Continuous Ranked Probability Score (CRPS) is the square difference between the empirical distribution function
 * of an ensemble forecast and the step function associated with a single-valued observation, integrated over the unit
 * interval. By convention, the CRPS is then averaged over each pair of ensemble forecasts and observations. Optionally,
 * the CRPS may be factored into a three-component decomposition, {@link MetricDecompositionGroup#CR}.
 * </p>
 * <p>
 * Uses the procedure outlined in Hersbach, H. (2000) Decomposition of the Continuous Ranked Probability Score for
 * Ensemble Prediction Systems. <i>Weather and Forecasting</i>, <b>15</b>(5) pp. 559-570. When the inputs contain
 * ensemble forecasts with a varying number of ensemble members, the pairs are split into groups with an equal number of
 * members, and a weighed average CRPS is computed across the groups, based on the fraction of pairs in each group.
 * </p>
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class ContinuousRankedProbabilityScore extends Metric<EnsemblePairs, VectorOutput>
implements ProbabilityScore
{

    /**
     * The decomposition identifier.
     */

    private final MetricDecompositionGroup decompositionID;

    @Override
    public VectorOutput apply(EnsemblePairs s)
    {
        switch(getDecompositionID())
        {
            case NONE:
                return getCRPSNoDecomp(s);
            case CR:
            case LBR:
            case CR_AND_LBR:
            default:
                throw new UnsupportedOperationException("The CRPS decomposition is not currently " + "implemented.");
        }
    }

    @Override
    public boolean isDecomposable()
    {
        return true;
    }

    @Override
    public MetricDecompositionGroup getDecompositionID()
    {
        return decompositionID;
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SCORE;
    }

    @Override
    public boolean isSkillScore()
    {
        return false;
    }

    @Override
    public boolean isProper()
    {
        return true;
    }

    @Override
    public boolean isStrictlyProper()
    {
        return true;
    }

    @Override
    public boolean hasRealUnits()
    {
        return true;
    }

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    static class CRPSBuilder extends MetricBuilder<EnsemblePairs, VectorOutput>
    {
        /**
         * The type of metric decomposition.
         */

        private MetricDecompositionGroup decompositionID = MetricDecompositionGroup.NONE;

        @Override
        protected ContinuousRankedProbabilityScore build()
        {
            return new ContinuousRankedProbabilityScore(this);
        }

        /**
         * Sets the decomposition identifier.
         * 
         * @param decompositionID the decomposition identifier
         * @return the builder
         */

        protected CRPSBuilder setDecompositionID(final MetricDecompositionGroup decompositionID)
        {
            this.decompositionID = decompositionID;
            return this;
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     */

    private ContinuousRankedProbabilityScore(final CRPSBuilder builder)
    {
        super(builder);
        if(builder.decompositionID != MetricDecompositionGroup.CR)
        {
            throw new IllegalArgumentException("Unsupported decomposition identifier: " + builder.decompositionID);
        }
        this.decompositionID = builder.decompositionID;
    }

    /**
     * Returns the CRPS without any decomposition using the procedure outlined in Hersbach, H. (2000).
     * 
     * @param s the pairs
     * @return the mean CRPS without decomposition
     */

    private VectorOutput getCRPSNoDecomp(final EnsemblePairs s)
    {
        double crps = 0.0;
        
        
        
        
        

        //Metadata
        final MetricOutputMetadata metOut = getMetadata(s, s.getData().size(), MetricConstants.MAIN, null);
        return getDataFactory().ofVectorOutput(new double[]{crps}, metOut);
    }

}
