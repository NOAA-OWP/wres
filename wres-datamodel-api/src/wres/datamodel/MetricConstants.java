package wres.datamodel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Metric constants. The metric identifiers are grouped by metric input/output type, as defined by the
 * {@link MetricInputGroup} and {@link MetricOutputGroup}, respectively.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public enum MetricConstants
{

    /**
     * Identifier for fractional bias or relative mean error.
     */

    BIAS_FRACTION( MetricInputGroup.SINGLE_VALUED, MetricOutputGroup.SCALAR ),

    /**
     * Identifier for a Brier Score.
     */

    BRIER_SCORE( MetricInputGroup.DISCRETE_PROBABILITY, MetricOutputGroup.VECTOR ),

    /**
     * Identifier for a Brier Skill Score.
     */

    BRIER_SKILL_SCORE( MetricInputGroup.DISCRETE_PROBABILITY, MetricOutputGroup.VECTOR ),

    /**
     * Identifier for coefficient of determination.
     */

    COEFFICIENT_OF_DETERMINATION( MetricInputGroup.SINGLE_VALUED, MetricOutputGroup.SCALAR ),

    /**
     * Identifier for a Mean Continuous Ranked Probability Score
     */

    CONTINUOUS_RANKED_PROBABILITY_SCORE( MetricInputGroup.ENSEMBLE, MetricOutputGroup.VECTOR ),

    /**
     * Identifier for a Mean Continuous Ranked Probability Skill Score
     */

    CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE( MetricInputGroup.ENSEMBLE, MetricOutputGroup.VECTOR ),

    /**
     * Identifier for a Contingency Table.
     */

    CONTINGENCY_TABLE( MetricInputGroup.MULTICATEGORY, MetricOutputGroup.MATRIX ),

    /**
     * Identifier for Pearson's product-moment correlation coefficient.
     */

    CORRELATION_PEARSONS( MetricInputGroup.SINGLE_VALUED, MetricOutputGroup.SCALAR ),

    /**
     * Identifier for a Critical Success Index.
     */

    CRITICAL_SUCCESS_INDEX( MetricInputGroup.DICHOTOMOUS, MetricOutputGroup.SCALAR ),

    /**
     * Identifier for an Equitable Threat Score.
     */

    EQUITABLE_THREAT_SCORE( MetricInputGroup.DICHOTOMOUS, MetricOutputGroup.SCALAR ),

    /**
     * Identifier for a Mean Absolute Error.
     */

    MEAN_ABSOLUTE_ERROR( MetricInputGroup.SINGLE_VALUED, MetricOutputGroup.SCALAR ),

    /**
     * Identifier for a Mean Error.
     */

    MEAN_ERROR( MetricInputGroup.SINGLE_VALUED, MetricOutputGroup.SCALAR ),

    /**
     * Identifier for a Mean Square Error.
     */

    MEAN_SQUARE_ERROR( MetricInputGroup.SINGLE_VALUED, MetricOutputGroup.VECTOR ),

    /**
     * Identifier for a Mean Square Error Skill Score.
     */

    MEAN_SQUARE_ERROR_SKILL_SCORE( MetricInputGroup.SINGLE_VALUED, MetricOutputGroup.VECTOR ),

    /**
     * Identifier for a Peirce Skill Score.
     */

    PEIRCE_SKILL_SCORE( MetricInputGroup.DICHOTOMOUS, MetricOutputGroup.SCALAR ),

    /**
     * Identifier for a Probability Of Detection.
     */

    PROBABILITY_OF_DETECTION( MetricInputGroup.DICHOTOMOUS, MetricOutputGroup.SCALAR ),

    /**
     * Identifier for a Probability Of False Detection.
     */

    PROBABILITY_OF_FALSE_DETECTION( MetricInputGroup.DICHOTOMOUS, MetricOutputGroup.SCALAR ),

    /**
     * Quantile-quantile diagram.
     */

    QUANTILE_QUANTILE_DIAGRAM( MetricInputGroup.SINGLE_VALUED, MetricOutputGroup.MULTIVECTOR ),

    /**
     * Identifier for the Relative Operating Characteristic.
     */

    RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM( MetricInputGroup.DISCRETE_PROBABILITY, MetricOutputGroup.MULTIVECTOR ),

    /**
     * Identifier for the Relative Operating Characteristic Score.
     */

    RELATIVE_OPERATING_CHARACTERISTIC_SCORE( MetricInputGroup.DISCRETE_PROBABILITY, MetricOutputGroup.VECTOR ),

    /**
     * Identifier for the Reliability Diagram.
     */

    RELIABILITY_DIAGRAM( MetricInputGroup.DISCRETE_PROBABILITY, MetricOutputGroup.MULTIVECTOR ),

    /**
     * Identifier for a Root Mean Square Error.
     */

    ROOT_MEAN_SQUARE_ERROR( MetricInputGroup.SINGLE_VALUED, MetricOutputGroup.SCALAR ),

    /**
     * Identifier for the sample size.
     */

    SAMPLE_SIZE( MetricInputGroup.SINGLE_VALUED, MetricInputGroup.ENSEMBLE, MetricOutputGroup.SCALAR ),

    /**
     * Indicator for no decomposition.
     */

    NONE( MetricDecompositionGroup.NONE ),

    /**
     * Identifier for a Calibration-Refinement (CR) factorization.
     */

    CR( MetricDecompositionGroup.CR ),

    /**
     * Identifier for a Calibration-Refinement (CR) factorization, together with an additional potential score
     * component.
     */

    CR_POT( MetricDecompositionGroup.CR_POT ),

    /**
     * Identifier for a Likeilihood-Base-Rate (LBR) factorization.
     */

    LBR( MetricDecompositionGroup.LBR ),

    /**
     * Identifier for the score and components of both the CR and LBR factorizations.
     */

    CR_AND_LBR( MetricDecompositionGroup.CR_AND_LBR ),

    /**
     * Identifier for the main component of a metric, such as the overall score in a score decomposition.
     */

    MAIN( MetricDecompositionGroup.NONE, MetricDecompositionGroup.CR, MetricDecompositionGroup.CR_POT, MetricDecompositionGroup.LBR, MetricDecompositionGroup.CR_AND_LBR ),

    /**
     * Identifier for the reliability component of a score decomposition.
     */

    RELIABILITY( MetricDecompositionGroup.CR, MetricDecompositionGroup.CR_POT, MetricDecompositionGroup.CR_AND_LBR ),

    /**
     * Identifier for the resolution component of a score decomposition.
     */

    RESOLUTION( MetricDecompositionGroup.CR, MetricDecompositionGroup.CR_POT, MetricDecompositionGroup.CR_AND_LBR ),

    /**
     * Identifier for the uncertainty component of a score decomposition.
     */

    UNCERTAINTY( MetricDecompositionGroup.CR, MetricDecompositionGroup.CR_POT, MetricDecompositionGroup.CR_AND_LBR ),

    /**
     * Identifier for the potential score value (perfect reliability).
     */

    POTENTIAL( MetricDecompositionGroup.CR_POT ),

    /**
     * Identifier for the Type-II conditional bias component of a score decomposition.
     */

    TYPE_II_BIAS( MetricDecompositionGroup.LBR, MetricDecompositionGroup.CR_AND_LBR ),

    /**
     * Identifier for the discrimination component of a score decomposition.
     */

    DISCRIMINATION( MetricDecompositionGroup.LBR, MetricDecompositionGroup.CR_AND_LBR ),

    /**
     * Identifier for the sharpness component of a score decomposition.
     */

    SHARPNESS( MetricDecompositionGroup.LBR, MetricDecompositionGroup.CR_AND_LBR ),

    /**
     * Identifier for a forecast probability.
     */

    FORECAST_PROBABILITY,

    /**
     * Identifier for the conditional observed probability of an event, given the forecast probability.
     */

    OBSERVED_GIVEN_FORECAST_PROBABILITY,

    /**
     * Identifier for predicted quantiles.
     */

    PREDICTED_QUANTILES,

    /**
     * Identifier for observed quantiles.
     */

    OBSERVED_QUANTILES;

    /**
     * The {@link MetricInputGroup} or null if the {@link MetricConstants} does not belong to a group.
     */

    private final MetricInputGroup[] inGroup;

    /**
     * The {@link MetricOutputGroup} or null if the {@link MetricConstants} does not belong to a group.
     */

    private final MetricOutputGroup outGroup;

    /**
     * The {@link MetricDecompositionGroup} to which this {@link MetricConstants} belongs or null if the
     * {@link MetricConstants} does not belong to a {@link MetricDecompositionGroup}.
     */

    private final MetricDecompositionGroup[] decGroup;

    /**
     * Default constructor
     */

    private MetricConstants()
    {
        inGroup = null;
        outGroup = null;
        decGroup = null;
    }

    /**
     * Construct with a {@link MetricInputGroup} and a {@link MetricOutputGroup}.
     * 
     * @param inputGroup the input group
     * @param outputGroup the output group
     */

    private MetricConstants( MetricInputGroup inGroup, MetricOutputGroup outGroup )
    {
        this.inGroup = new MetricInputGroup[] { inGroup };
        this.outGroup = outGroup;
        decGroup = null;
    }

    /**
     * Construct with two {@link MetricInputGroup} and a {@link MetricOutputGroup}.
     * 
     * @param firstGroup the first input group
     * @param secondGroup the second input group
     * @param outputGroup the output group
     */

    private MetricConstants( MetricInputGroup firstGroup, MetricInputGroup secondGroup, MetricOutputGroup outGroup )
    {
        this.inGroup = new MetricInputGroup[] { firstGroup, secondGroup };
        this.outGroup = outGroup;
        decGroup = null;
    }

    /**
     * Construct with a varargs of {@link MetricDecompositionGroup}.
     * 
     * @param decGroup the decomposition groups to which the {@link MetricConstants} belongs
     */

    private MetricConstants( MetricDecompositionGroup... decGroup )
    {
        this.decGroup = decGroup;
        inGroup = null;
        outGroup = null;
    }

    /**
     * Returns true if the input {@link MetricInputGroup} contains the current {@link MetricConstants}, false otherwise.
     * 
     * @param inGroup the {@link MetricInputGroup}
     * @return true if the input {@link MetricInputGroup} contains the current {@link MetricConstants}, false otherwise
     */

    public boolean isInGroup( MetricInputGroup inGroup )
    {
        return Arrays.asList( this.inGroup ).contains( inGroup );
    }

    /**
     * Returns true if the input {@link MetricOutputGroup} contains the current {@link MetricConstants}, false
     * otherwise.
     * 
     * @param outGroup the {@link MetricOutputGroup}
     * @return true if the input {@link MetricOutputGroup} contains the current {@link MetricConstants}, false otherwise
     */

    public boolean isInGroup( MetricOutputGroup outGroup )
    {
        return this.outGroup == outGroup;
    }

    /**
     * Returns true if the input {@link MetricInputGroup} and {@link MetricOutputGroup} both contain the current
     * {@link MetricConstants}, false otherwise.
     * 
     * @param inGroup the {@link MetricInputGroup}
     * @param outGroup the {@link MetricOutputGroup}
     * @return true if the input {@link MetricInputGroup} and {@link MetricOutputGroup} and both contain the current
     *         {@link MetricConstants}, false otherwise
     */

    public boolean isInGroup( MetricInputGroup inGroup, MetricOutputGroup outGroup )
    {
        return isInGroup( inGroup ) && isInGroup( outGroup );
    }

    /**
     * Returns all metric components in the {@link MetricDecompositionGroup} with which this constant is associated or
     * null if none is defined.
     * 
     * @return the components in the {@link MetricDecompositionGroup} or null
     */

    public List<MetricConstants> getMetricComponents()
    {
        return Objects.isNull( decGroup ) ? null : decGroup[0].getMetricComponents();
    }

    /**
     * Returns all {@link MetricConstants} associated with the specified {@link MetricInputGroup} and
     * {@link MetricOutputGroup}.
     * 
     * @param inGroup the {@link MetricInputGroup}
     * @param outGroup the {@link MetricOutputGroup}
     * @return the {@link MetricConstants} associated with the current {@link MetricInputGroup}
     */

    public static Set<MetricConstants> getMetrics( MetricInputGroup inGroup, MetricOutputGroup outGroup )
    {
        Set<MetricConstants> all = EnumSet.allOf( MetricConstants.class );
        all.removeIf( a -> Objects.isNull( a.inGroup ) || !Arrays.asList( a.inGroup ).contains( inGroup )
                           || a.outGroup != outGroup );
        return all;
    }

    /**
     * Type of metric input.
     */

    public enum MetricInputGroup
    {

        /**
         * Metrics that consume single-valued inputs.
         */

        SINGLE_VALUED,

        /**
         * Metrics that consume discrete probability inputs.
         */

        DISCRETE_PROBABILITY,

        /**
         * Metrics that consume dichotomous inputs.
         */

        DICHOTOMOUS,

        /**
         * Metrics that consume multi-category inputs.
         */

        MULTICATEGORY,

        /**
         * Metrics that consume ensemble inputs.
         */

        ENSEMBLE;

        /**
         * Returns all {@link MetricConstants} associated with the current {@link MetricInputGroup}.
         * 
         * @return the {@link MetricConstants} associated with the current {@link MetricInputGroup}
         */

        public Set<MetricConstants> getMetrics()
        {
            Set<MetricConstants> all = EnumSet.allOf( MetricConstants.class );
            all.removeIf( a -> Objects.isNull( a.inGroup ) || !Arrays.asList( a.inGroup ).contains( this ) );
            return all;
        }

        /**
         * Returns true if this {@link MetricInputGroup} contains the input {@link MetricConstants}, false otherwise.
         * 
         * @param input the {@link MetricConstants} to test
         * @return true if this {@link MetricInputGroup} contains the input {@link MetricConstants}, false otherwise
         */

        public boolean contains( MetricConstants input )
        {
            return getMetrics().contains( input );
        }

    }

    /**
     * Type of metric output.
     */

    public enum MetricOutputGroup
    {

        /**
         * Metrics that produce a scalar output.
         */

        SCALAR,

        /**
         * Metrics that produce a vector of outputs.
         */

        VECTOR,

        /**
         * Metrics that produce multiple vectors of outputs.
         */

        MULTIVECTOR,

        /**
         * Metrics that produce matrix outputs.
         */

        MATRIX;

        /**
         * Returns all {@link MetricConstants} associated with the current {@link MetricOutputGroup}.
         * 
         * @return the {@link MetricConstants} associated with the current {@link MetricOutputGroup}
         */

        public Set<MetricConstants> getMetrics()
        {
            Set<MetricConstants> all = EnumSet.allOf( MetricConstants.class );
            all.removeIf( a -> a.outGroup != this );
            return all;
        }

        /**
         * Returns true if this {@link MetricOutputGroup} contains the input {@link MetricConstants}, false otherwise.
         * 
         * @param input the {@link MetricConstants} to test
         * @return true if this {@link MetricOutputGroup} contains the input {@link MetricConstants}, false otherwise
         */

        public boolean contains( MetricConstants input )
        {
            return getMetrics().contains( input );
        }

    }

    /**
     * Type of metric decomposition.
     */

    public enum MetricDecompositionGroup
    {

        /**
         * Indicator for no decomposition.
         */

        NONE,

        /**
         * Identifier for a Calibration-Refinement (CR) factorization into reliability - resolution + uncertainty,
         * comprising the overall score followed by the three components in that order.
         */

        CR,

        /**
         * Identifier for a Calibration-Refinement (CR) factorization into reliability - resolution + uncertainty,
         * comprising the overall score followed by the three components in that order, together with an additional
         * potential score component, where potential = actual score - reliability.
         */

        CR_POT,

        /**
         * Identifier for a Likelihood-Base-Rate (LBR) factorization into Type-II conditional bias - discrimination +
         * sharpness, comprising the overall score followed by the three components in that order.
         */

        LBR,

        /**
         * Identifier for the score and components of both the CR and LBR factorizations.
         */

        CR_AND_LBR;

        /**
         * Returns all {@link MetricConstants} associated with the current {@link MetricDecompositionGroup}.
         * 
         * @return the {@link MetricConstants} associated with the current {@link MetricDecompositionGroup}
         */

        public List<MetricConstants> getMetricComponents()
        {
            List<MetricConstants> all = new ArrayList<>( EnumSet.allOf( MetricConstants.class ) );
            //Remove constants with the same string trace across MetricConstants and MetricDecompositionGroup
            all.removeIf( a -> Objects.isNull( a.decGroup ) || a.toString().equals( toString() )
                               || !Arrays.asList( a.decGroup ).contains( this ) );
            return all;
        }

        /**
         * Returns true if this {@link MetricDecompositionGroup} contains the input {@link MetricConstants}, false
         * otherwise.
         * 
         * @param input the {@link MetricConstants} to test
         * @return true if this {@link MetricDecompositionGroup} contains the input {@link MetricConstants}, false
         *         otherwise
         */

        public boolean contains( MetricConstants input )
        {
            return getMetricComponents().contains( input );
        }

    }

}
