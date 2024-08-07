package wres.metrics.discreteprobability;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math3.util.Precision;

import wres.datamodel.types.Probability;
import wres.config.MetricConstants;
import wres.config.MetricConstants.MetricGroup;
import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.metrics.FunctionFactory;
import wres.metrics.ProbabilityScore;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * <p>
 * Computes the area underneath the {@link RelativeOperatingCharacteristicDiagram} (AUC). The
 * {@link RelativeOperatingCharacteristicScore} reports the fractional gain against the AUC of a baseline. If no
 * baseline is provided, an unskillful baseline is assumed (AUC=0.5), for which the Relative Operating Characteristic
 * (ROC) score is given by: <code>ROC score = 2.0 * AUC-1</code>. When a baseline is provided, the skill is reported as
 * the fractional improvement in the AUC of the main prediction (<code>AUCm</code>) against the AUC of the baseline
 * prediction (<code>AUCb</code>), namely: <code>ROC score = (AUCm - AUCb) / (1.0 - AUCb).</code>
 * </p>
 * <p>
 * The AUC may be computed in several ways. Currently, the default follows the procedure outlined in Mason and Graham
 * (2002). When computing the ROC score from an ensemble forecasting system, this effectively reports the AUC for a ROC
 * curve derived from as many classifiers as ensemble members in the ensemble forecast.
 * </p>
 * <p>
 * Mason, S. J. and Graham, N. E. (2002) Areas beneath the relative operating characteristics (ROC) and relative
 * operating levels (ROL) curves: Statistical significance and interpretation, Q. J. R. Meteorol. Soc. 128, 2145-2166.
 * </p>
 *
 * @author James Brown
 */

public class RelativeOperatingCharacteristicScore
        implements ProbabilityScore<Pool<Pair<Probability, Probability>>, DoubleScoreStatisticOuter>
{

    /**
     * Basic description of the metric.
     */

    public static final DoubleScoreMetric BASIC_METRIC = DoubleScoreMetric.newBuilder()
                                                                          .setName( MetricName.RELATIVE_OPERATING_CHARACTERISTIC_SCORE )
                                                                          .build();

    /**
     * Main score component.
     */

    public static final DoubleScoreMetricComponent MAIN =
            DoubleScoreMetricComponent.newBuilder()
                                      .setMinimum( MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_SCORE.getMinimum() )
                                      .setMaximum( MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_SCORE.getMaximum() )
                                      .setOptimum( MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_SCORE.getOptimum() )
                                      .setName( MetricName.MAIN )
                                      .setUnits( MeasurementUnit.DIMENSIONLESS )
                                      .build();

    /**
     * Full description of the metric.
     */

    public static final DoubleScoreMetric METRIC = DoubleScoreMetric.newBuilder()
                                                                    .addComponents( RelativeOperatingCharacteristicScore.MAIN )
                                                                    .setName( MetricName.RELATIVE_OPERATING_CHARACTERISTIC_SCORE )
                                                                    .build();

    /** Whether to use a baseline dataset to calculate a skill score formulation. */
    private final boolean useBaseline;

    /**
     * Returns an instance.
     *
     * @return an instance
     */

    public static RelativeOperatingCharacteristicScore of()
    {
        return new RelativeOperatingCharacteristicScore( true );
    }

    /**
     * Returns an instance with control on whether a baseline is used to calculate a skill formulation.
     *
     * @param useBaseline is true to calculate a skill formulation when a baseline is present, false for a raw score
     * @return an instance
     */

    public static RelativeOperatingCharacteristicScore of( boolean useBaseline )
    {
        return new RelativeOperatingCharacteristicScore( useBaseline );
    }

    @Override
    public DoubleScoreStatisticOuter apply( final Pool<Pair<Probability, Probability>> pool )
    {
        if ( Objects.isNull( pool ) )
        {
            throw new PoolException( "Specify non-null input to the '" + this + "'." );
        }
        // Obtain the AUC for the main prediction and, if available, the baseline.
        double rocScore;

        if ( this.useBaseline
             && pool.hasBaseline() )
        {
            double rocMain = this.getAUCMasonGraham( pool );
            double rocBase = this.getAUCMasonGraham( pool.getBaselineData() );
            rocScore = ( rocMain - rocBase ) / ( 1.0 - rocBase );
        }
        else
        {
            rocScore = 2.0 * this.getAUCMasonGraham( pool ) - 1.0;
        }

        DoubleScoreStatisticComponent component =
                DoubleScoreStatisticComponent.newBuilder()
                                             .setMetric( RelativeOperatingCharacteristicScore.MAIN )
                                             .setValue( rocScore )
                                             .build();
        DoubleScoreStatistic score =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( RelativeOperatingCharacteristicScore.BASIC_METRIC )
                                    .addStatistics( component )
                                    .build();

        return DoubleScoreStatisticOuter.of( score, pool.getMetadata() );
    }

    @Override
    public MetricConstants getMetricName()
    {
        return MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_SCORE;
    }

    @Override
    public boolean isDecomposable()
    {
        return false;
    }


    @Override
    public boolean isProper()
    {
        return false;
    }

    @Override
    public boolean isStrictlyProper()
    {
        return false;
    }

    @Override
    public boolean hasRealUnits()
    {
        return false;
    }

    @Override
    public MetricGroup getScoreOutputGroup()
    {
        return MetricGroup.NONE;
    }

    @Override
    public String toString()
    {
        return this.getMetricName()
                   .toString();
    }

    /**
     * Returns the AUC using the procedure outlined in Mason and graham (2002).
     *
     * @param pairs the pairs
     * @return the AUC
     */

    private double getAUCMasonGraham( Pool<Pair<Probability, Probability>> pairs )
    {
        // Obtain the predicted probabilities when the event occurred and did not occur
        List<Probability> byOccurrence = new ArrayList<>();
        List<Probability> byNonOccurrence = new ArrayList<>();
        for ( Pair<Probability, Probability> nextPair : pairs.get() )
        {
            Probability left = nextPair.getLeft();
            Probability right = nextPair.getRight();

            // Occurrence
            if ( Precision.equals( left.getProbability(),
                                   1.0,
                                   Precision.EPSILON ) )
            {
                byOccurrence.add( right );
            }
            // Non-occurrence
            else
            {
                byNonOccurrence.add( right );
            }
        }

        // Score is undefined
        if ( byOccurrence.isEmpty() || byNonOccurrence.isEmpty() )
        {
            return Double.NaN;
        }

        // Sort descending
        byOccurrence.sort( Collections.reverseOrder() );
        byNonOccurrence.sort( Collections.reverseOrder() );

        // For each occurrence, determine how many forecasts associated with non-occurrences had a larger or equal
        // probability. Derive the AUC from this.
        double rhs = 0.0;
        for ( Probability probYes : byOccurrence )
        {
            for ( Probability probNo : byNonOccurrence )
            {
                double diff = probNo.getProbability() - probYes.getProbability();
                if ( diff > .0000001 )
                { // prob[non-occurrence] > prob[occurrence]
                    rhs += 2.0;
                }
                else if ( Math.abs( diff ) < .0000001 )
                { // Equal probs
                    rhs += 1.0;
                }
                else
                { // Less than
                    break; //Sorted data, so no more elements
                }
            }
        }

        return FunctionFactory.finiteOrMissing()
                              .applyAsDouble( 1.0 -
                                              ( ( 1.0 / ( 2.0 * byOccurrence.size() * byNonOccurrence.size() ) )
                                                * rhs ) );
    }

    /**
     * Hidden constructor.
     * @param useBaseline is true to use a baseline dataset in a skill formulation, false otherwise
     */

    private RelativeOperatingCharacteristicScore( boolean useBaseline )
    {
        super();
        this.useBaseline = useBaseline;
    }

}
