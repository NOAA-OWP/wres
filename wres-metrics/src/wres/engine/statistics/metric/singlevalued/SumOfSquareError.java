package wres.engine.statistics.metric.singlevalued;

import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricGroup;
import wres.datamodel.MissingValues;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.engine.statistics.metric.Collectable;
import wres.engine.statistics.metric.DecomposableScore;
import wres.engine.statistics.metric.FunctionFactory;
import wres.engine.statistics.metric.MetricParameterException;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent.ComponentName;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Base class for decomposable scores that involve a sum-of-square errors.
 * 
 * @author james.brown@hydrosolved.com
 */
public class SumOfSquareError extends DecomposableScore<SampleData<Pair<Double, Double>>>
        implements Collectable<SampleData<Pair<Double, Double>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter>
{

    /**
     * Basic description of the metric.
     */

    public static final DoubleScoreMetric BASIC_METRIC = DoubleScoreMetric.newBuilder()
                                                                          .setName( MetricName.SUM_OF_SQUARE_ERROR )
                                                                          .build();

    /**
     * Main score component.
     */

    public static final DoubleScoreMetricComponent MAIN = DoubleScoreMetricComponent.newBuilder()
                                                                                    .setMinimum( 0 )
                                                                                    .setMaximum( Double.POSITIVE_INFINITY )
                                                                                    .setOptimum( 0 )
                                                                                    .setName( ComponentName.MAIN )
                                                                                    .build();

    /**
     * Full description of the metric.
     */

    public static final DoubleScoreMetric METRIC = DoubleScoreMetric.newBuilder()
                                                                    .addComponents( SumOfSquareError.MAIN )
                                                                    .setName( MetricName.SUM_OF_SQUARE_ERROR )
                                                                    .build();

    /**
     * Returns an instance.
     * 
     * @return an instance
     */

    public static SumOfSquareError of()
    {
        return new SumOfSquareError();
    }

    @Override
    public DoubleScoreStatisticOuter apply( SampleData<Pair<Double, Double>> s )
    {
        return this.aggregate( this.getInputForAggregation( s ) );
    }

    @Override
    public boolean hasRealUnits()
    {
        return true;
    }

    @Override
    public boolean isSkillScore()
    {
        return false;
    }

    @Override
    public MetricConstants getMetricName()
    {
        return MetricConstants.SUM_OF_SQUARE_ERROR;
    }

    @Override
    public DoubleScoreStatisticOuter getInputForAggregation( SampleData<Pair<Double, Double>> input )
    {
        if ( Objects.isNull( input ) )
        {
            throw new SampleDataException( "Specify non-null input to the '" + this + "'." );
        }

        double returnMe = MissingValues.DOUBLE;

        // Data available
        if ( !input.getRawData().isEmpty() )
        {
            // Sort the stream, as this improves accuracy according to the API docs
            // See #71343
            returnMe = input.getRawData()
                            .stream()
                            .mapToDouble( FunctionFactory.squareError() )
                            .sorted()
                            .sum();
        }

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( SumOfSquareError.MAIN )
                                                                               .setValue( returnMe )
                                                                               .build();

        DoubleScoreStatistic score =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( SumOfSquareError.BASIC_METRIC )
                                    .addStatistics( component )
                                    .setSampleSize( input.getRawData().size() )
                                    .build();

        return DoubleScoreStatisticOuter.of( score, input.getMetadata() );
    }

    @Override
    public DoubleScoreStatisticOuter aggregate( DoubleScoreStatisticOuter output )
    {
        if ( Objects.isNull( output ) )
        {
            throw new SampleDataException( "Specify non-null input to the '" + this + "'." );
        }

        return DoubleScoreStatisticOuter.of( output.getData(), output.getMetadata() );
    }

    @Override
    public MetricConstants getCollectionOf()
    {
        return MetricConstants.SUM_OF_SQUARE_ERROR;
    }

    /**
     * Hidden constructor.
     */

    SumOfSquareError()
    {
        super();
    }

    /**
     * Hidden constructor.
     * 
     * @param decompositionId the decomposition identifier
     * @throws MetricParameterException if one or more parameters is invalid 
     */

    SumOfSquareError( MetricGroup decompositionId ) throws MetricParameterException
    {
        super( decompositionId );
    }

}
