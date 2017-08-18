package wres.engine.statistics.metric;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;

import wres.datamodel.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.metric.EnsemblePairs;
import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.MetricConstants.MetricDecompositionGroup;
import wres.datamodel.metric.MetricInputException;
import wres.datamodel.metric.MetricOutputMetadata;
import wres.datamodel.metric.Slicer;
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
class ContinuousRankedProbabilityScore extends Metric<EnsemblePairs, VectorOutput> implements ProbabilityScore
{

    /**
     * The decomposition identifier.
     */

    private final MetricDecompositionGroup decompositionID;

    @Override
    public VectorOutput apply(EnsemblePairs s)
    {
        if(Objects.isNull(s))
        {
            throw new MetricInputException("Specify non-null input to the '" + this + "'.");
        }
        //Slice the data into groups with an equal number of ensemble members
        Slicer slicer = getDataFactory().getSlicer();
        Map<Integer, List<PairOfDoubleAndVectorOfDoubles>> sliced = slicer.sliceByRight(s.getData());

        //CRPS, currently without decomposition
        double[] crps = new double[1];
        sliced.values().forEach(pairs -> {
            switch(getDecompositionID())
            {
                case NONE:
                    crps[0] += getSumCRPSNoDecomp(pairs);
                    break;
                case CR:
                default:
                    throw new UnsupportedOperationException("The CRPS decomposition is not currently implemented.");
            }
        });
        //Compute the average (implicitly weighted by the number of pairs in each group)
        crps[0] = crps[0] / s.size();
        //Metadata
        final MetricOutputMetadata metOut = getMetadata(s, s.getData().size(), MetricConstants.MAIN, null);
        return getDataFactory().ofVectorOutput(crps, metOut);
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
     * Returns the sum of the CRPS values for the individual pairs, without any decomposition. Uses the procedure
     * outlined in Hersbach, H. (2000). Requires an equal number of ensemble members in each pair.
     * 
     * @param s the pairs
     * @return the mean CRPS without decomposition
     */

    private double getSumCRPSNoDecomp(final List<PairOfDoubleAndVectorOfDoubles> s)
    {
        //Number of ensemble members
        int members = s.get(0).getItemTwo().length;

        double totCRPS = 0.0;

        //Iterate through the member positions and determine the mean alpha and beta      
        for(int i = 0; i < members + 1; i++)
        {
            Incrementer incrementer = new Incrementer(i, members);
            for(PairOfDoubleAndVectorOfDoubles nextPair: s)
            {
                sumAlphaBeta().accept(nextPair, incrementer);
            }
            totCRPS += incrementer.totCRPS;
            //TODO: increment the alpha and beta here when decompositions are implemented
        }
        return totCRPS;
    }

    /**
     * Returns a consumer that increments the parameters within an {@link Incrementer} for each input pair. TODO:
     * increment the alphas and betas when decompositions are implemented
     * 
     * @return a consumer that increments the parameters in the {@link Incrementer} for each input pair
     */

    private static BiConsumer<PairOfDoubleAndVectorOfDoubles, Incrementer> sumAlphaBeta()
    {
        return (nextPair, inc) -> {
            //Observation 
            double obs = nextPair.getItemOne();
            double[] forecast = nextPair.getItemTwo();
            //Sort the forecast
            Arrays.sort(forecast);
            if(inc.member == 0)
            {
                //Deal with low outlier: case 1
                if(obs < forecast[0])
                {
                    final double nextBeta = forecast[0] - obs;
//                    inc.betaSum += nextBeta; //Alpha unchanged
                    inc.totCRPS += (nextBeta * inc.invProbSquared);
                }
            }
            //Deal with high outlier: case 2
            else if(inc.member == inc.totalMembers)
            {
                if(obs > forecast[inc.totalMembers - 1])
                {
                    final double nextAlpha = obs - forecast[inc.totalMembers - 1];
//                    inc.alphaSum += nextAlpha; //Beta unchanged
                    inc.totCRPS += (nextAlpha * inc.probSquared);
                }
            }
            //Deal with remaining 3 cases, for 0 < i < N
            else
            {
                //Case 3: observed exceeds ith
                if(obs > forecast[inc.member])
                {
                    final double nextAlpha = forecast[inc.member] - forecast[inc.member - 1];
//                    inc.alphaSum += nextAlpha; //Beta unchanged
                    inc.totCRPS += nextAlpha * inc.probSquared;
                } //Case 4: observed falls below i-1th
                else if(obs < forecast[inc.member - 1])
                {
                    final double nextBeta = forecast[inc.member] - forecast[inc.member - 1];
//                    inc.betaSum += nextBeta; //Alpha unchanged
                    inc.totCRPS += nextBeta * inc.invProbSquared;
                } //Case 5: observed falls between i-1th and ith
                else
                {
                    final double nextAlpha = obs - forecast[inc.member - 1];
                    final double nextBeta = forecast[inc.member] - obs;
//                    inc.alphaSum += nextAlpha;
//                    inc.betaSum += nextBeta;
                    inc.totCRPS += ((nextAlpha * inc.probSquared) + (nextBeta * inc.invProbSquared));
                }
            }
        };
    }

    /**
     * Class to increment CRPS components.
     */

    private static class Incrementer
    {

        /**
         * Member number.
         */

        private final int member;

        /**
         * Total number of members.
         */

        private final int totalMembers;

        /**
         * Probability.
         */

        private final double prob;

        /**
         * Probability squared.
         */

        private final double probSquared;

        /**
         * Inverse probability squared.
         */

        private final double invProbSquared;

        /**
         * The incremented total CRPS
         */

        private double totCRPS = 0.0;

//        /**
//         * The incremented alpha parameter.
//         */
//        
//        private double alphaSum = 0.0;
//        
//        /**
//         * The incremented beta parameter.
//         */
//        
//        private double betaSum = 0.0;

        /**
         * Construct the incrementer.
         */

        private Incrementer(int member, int totalMembers)
        {
            this.member = member;
            this.totalMembers = totalMembers;
            prob = ((double)member) / totalMembers;
            probSquared = prob * prob;
            invProbSquared = Math.pow(1.0 - prob, 2);
        }

    }

}
