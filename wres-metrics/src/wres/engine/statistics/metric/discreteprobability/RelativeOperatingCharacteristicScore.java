package wres.engine.statistics.metric.discreteprobability;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.DiscreteProbabilityPairs;
import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.engine.statistics.metric.FunctionFactory;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.OrdinaryScore;
import wres.engine.statistics.metric.ProbabilityScore;

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
 * @author james.brown@hydrosolved.com
 */

public class RelativeOperatingCharacteristicScore extends OrdinaryScore<DiscreteProbabilityPairs, DoubleScoreOutput>
        implements ProbabilityScore<DiscreteProbabilityPairs,DoubleScoreOutput>
{

    @Override
    public DoubleScoreOutput apply( final DiscreteProbabilityPairs s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new MetricInputException( "Specify non-null input to the '" + this + "'." );
        }
        //Obtain the AUC for the main prediction and, if available, the baseline.
        double rocScore;
        if ( s.hasBaseline() )
        {
            double rocMain = getAUCMasonGraham( s );
            double rocBase = getAUCMasonGraham( s.getBaselineData() );
            rocScore = ( rocMain - rocBase ) / ( 1.0 - rocBase );
        }
        else
        {
            rocScore = 2.0 * getAUCMasonGraham( s ) - 1.0;
        }
        final MetricOutputMetadata metOut = getMetadata( s, s.getRawData().size(), MetricConstants.MAIN, null );
        return getDataFactory().ofDoubleScoreOutput( rocScore, metOut );
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_SCORE;
    }

    @Override
    public boolean isSkillScore()
    {
        return true;
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
    public ScoreOutputGroup getScoreOutputGroup()
    {
        return ScoreOutputGroup.NONE;
    }

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    public static class RelativeOperatingCharacteristicScoreBuilder
            extends
            OrdinaryScoreBuilder<DiscreteProbabilityPairs, DoubleScoreOutput>
    {

        @Override
        public RelativeOperatingCharacteristicScore build() throws MetricParameterException
        {
            return new RelativeOperatingCharacteristicScore( this );
        }

    }

    /**
     * Returns the AUC using the procedure outlined in Mason and graham (2002).
     * 
     * @param pairs the pairs
     * @return the AUC
     */

    private double getAUCMasonGraham( DiscreteProbabilityPairs pairs )
    {
        DataFactory d = getDataFactory();
        //Obtain the predicted probabilities when the event occurred and did not occur
        //Begin by collecting against occurrence/non-occurrence
        Map<Boolean, List<PairOfDoubles>> mapped = pairs.getRawData()
                                                        .stream()
                                                        .collect( Collectors.groupingBy( a -> d.doubleEquals( a.getItemOne(),
                                                                                                              1.0,
                                                                                                              7 ) ) );
        if ( mapped.size() != 2 )
        {
            return Double.NaN; //Undefined
        }
        //Get the right side by each outcome
        List<Double> byOccurrence =
                mapped.get( true ).stream().map( PairOfDoubles::getItemTwo ).collect( Collectors.toList() );
        List<Double> byNonOccurrence = mapped.get( false )
                                             .stream()
                                             .map( PairOfDoubles::getItemTwo )
                                             .collect( Collectors.toList() );
        //Sort descending
        Collections.sort( byOccurrence, Collections.reverseOrder() );
        Collections.sort( byNonOccurrence, Collections.reverseOrder() );

        //For each occurrence, determine how may forecasts associated with non-occurrences had a larger or equal 
        //probability. Derive the AUC from this.
        double rhs = 0.0;
        for ( double probYes : byOccurrence )
        {
            for ( double probNo : byNonOccurrence )
            {
                double diff = probNo - probYes;
                if ( diff > .0000001 )
                { //prob[non-occurrence] > prob[occurrence]
                    rhs += 2.0;
                }
                else if ( Math.abs( diff ) < .0000001 )
                { //Equal probs
                    rhs += 1.0;
                }
                else
                { //Less than
                    break; //Sorted data, so no more elements
                }
            }
        }
        return FunctionFactory.finiteOrMissing()
                              .applyAsDouble( 1.0 - ( ( 1.0 / ( 2.0 * byOccurrence.size() * byNonOccurrence.size() ) )
                                                      * rhs ) );
    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    private RelativeOperatingCharacteristicScore( final RelativeOperatingCharacteristicScoreBuilder builder )
            throws MetricParameterException
    {
        super( builder );
    }

}
