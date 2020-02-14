package wres.engine.statistics.metric.singlevalued;

import java.util.Objects;
import java.util.concurrent.atomic.DoubleAdder;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricGroup;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.StatisticMetadata;
import wres.engine.statistics.metric.DoubleErrorFunction;
import wres.engine.statistics.metric.FunctionFactory;

/**
 * Computes the mean error of a single-valued prediction as a fraction of the mean observed value.
 * 
 * @author james.brown@hydrosolved.com
 */
public class BiasFraction extends DoubleErrorScore<SampleData<Pair<Double, Double>>>
{

    /**
     * Returns an instance.
     * 
     * @return an instance
     */

    public static BiasFraction of()
    {
        return new BiasFraction();
    }

    @Override
    public DoubleScoreStatistic apply( SampleData<Pair<Double, Double>> s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new SampleDataException( "Specify non-null input to the '" + this + "'." );
        }
        final StatisticMetadata metOut =
                StatisticMetadata.of( s.getMetadata(),
                                      this.getID(),
                                      MetricConstants.MAIN,
                                      this.hasRealUnits(),
                                      s.getRawData().size(),
                                      null );
        DoubleAdder left = new DoubleAdder();
        DoubleAdder right = new DoubleAdder();
        DoubleErrorFunction error = FunctionFactory.error();
        s.getRawData().forEach( pair -> {
            left.add( error.applyAsDouble( pair ) );
            right.add( pair.getLeft() );
        } );
        double result = left.sum() / right.sum();
        //Set NaN if not finite
        if ( !Double.isFinite( result ) )
        {
            result = Double.NaN;
        }
        return DoubleScoreStatistic.of( result, metOut );
    }

    @Override
    public boolean isSkillScore()
    {
        return false;
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.BIAS_FRACTION;
    }

    @Override
    public boolean isDecomposable()
    {
        return false;
    }

    @Override
    public MetricGroup getScoreOutputGroup()
    {
        return MetricGroup.NONE;
    }

    @Override
    public boolean hasRealUnits()
    {
        return false;
    }

    /**
     * Hidden constructor.
     */

    private BiasFraction()
    {
        super();
    }

}
