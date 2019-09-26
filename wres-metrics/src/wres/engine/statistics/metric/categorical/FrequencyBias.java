package wres.engine.statistics.metric.categorical;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.MetricConstants;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.MatrixStatistic;
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
    public DoubleScoreStatistic aggregate( final MatrixStatistic output )
    {
        this.is2x2ContingencyTable( output, this );
        final MatrixStatistic v = output;
        final double[][] cm = v.getData().getDoubles();
        final double score =
                FunctionFactory.finiteOrMissing().applyAsDouble( ( cm[0][0] + cm[0][1] ) / ( cm[0][0] + cm[1][0] ) );
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
