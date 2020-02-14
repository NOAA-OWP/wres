package wres.engine.statistics.metric.categorical;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.MetricConstants;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.engine.statistics.metric.FunctionFactory;

/**
 * Measures the predicted fraction of occurrences against the observed fraction of occurrences. A ratio of 1.0 
 * indicates an absence of any bias in the predicted and observed frequencies with which an event occurs.
 * 
 * @author james.brown@hydrosolved.com
 */
public class FrequencyBias extends ContingencyTableScore
{
    
    /**
     * Returns an instance.
     * 
     * @return an instance
     */
    
    public static FrequencyBias of()
    {
        return new FrequencyBias();
    }    

    @Override
    public DoubleScoreStatistic apply( final SampleData<Pair<Boolean,Boolean>> s )
    {
        return aggregate( this.getInputForAggregation( s ) );
    }

    @Override
    public DoubleScoreStatistic aggregate( final DoubleScoreStatistic output )
    {
        this.is2x2ContingencyTable( output, this );

        double tP = output.getComponent( MetricConstants.TRUE_POSITIVES )
                          .getData();

        double fP = output.getComponent( MetricConstants.FALSE_POSITIVES )
                          .getData();

        double fN = output.getComponent( MetricConstants.FALSE_NEGATIVES )
                          .getData();
              
        final double score =
                FunctionFactory.finiteOrMissing().applyAsDouble( ( tP + fP ) / ( tP + fN ) );
        return DoubleScoreStatistic.of( score, getMetadata( output ) );
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.FREQUENCY_BIAS;
    }

    @Override
    public boolean isSkillScore()
    {
        return false;
    }

    /**
     * Hidden constructor.
     */

    private FrequencyBias()
    {
        super();
    }

}
