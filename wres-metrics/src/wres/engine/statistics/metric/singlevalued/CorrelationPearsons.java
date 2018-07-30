package wres.engine.statistics.metric.singlevalued;

import java.util.Objects;

import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;

import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.Slicer;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.engine.statistics.metric.Collectable;
import wres.engine.statistics.metric.FunctionFactory;
import wres.engine.statistics.metric.MetricCollection;
import wres.engine.statistics.metric.OrdinaryScore;

/**
 * Computes Pearson's product-moment correlation coefficient between the left and right sides of the {SingleValuedPairs}
 * input. Implements {@link Collectable} to avoid repeated calculations of derivative metrics, such as the
 * {@link CoefficientOfDetermination} when both appear in a {@link MetricCollection}.
 * 
 * @author james.brown@hydrosolved.com
 */
public class CorrelationPearsons extends OrdinaryScore<SingleValuedPairs, DoubleScoreOutput>
        implements Collectable<SingleValuedPairs, DoubleScoreOutput, DoubleScoreOutput>
{

    /**
     * Instance of {@link PearsonsCorrelation}.
     */

    private final PearsonsCorrelation correlation;

    /**
     * Returns an instance.
     * 
     * @return an instance
     */
    
    public static CorrelationPearsons of()
    {
        return new CorrelationPearsons();
    }
    
    @Override
    public DoubleScoreOutput apply( SingleValuedPairs s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new MetricInputException( "Specify non-null input to the '" + this + "'." );
        }

        Metadata in = s.getMetadata();
        // Set the metadata explicitly since this class implements Collectable and getID() may be overridden
        MetricOutputMetadata meta = MetadataFactory.getOutputMetadata( s.getRawData().size(),
                                                                       MetadataFactory.getDimension(),
                                                                       in.getDimension(),
                                                                       MetricConstants.PEARSON_CORRELATION_COEFFICIENT,
                                                                       MetricConstants.MAIN,
                                                                       in.getIdentifier() );
        double returnMe = Double.NaN;
        // Minimum sample size of 1
        if ( s.getRawData().size() > 1 )
        {
            returnMe = FunctionFactory.finiteOrMissing()
                                      .applyAsDouble( correlation.correlation( Slicer.getLeftSide( s ),
                                                                               Slicer.getRightSide( s ) ) );
        }
        return DataFactory.ofDoubleScoreOutput( returnMe, meta );
    }

    @Override
    public boolean isSkillScore()
    {
        return false;
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.PEARSON_CORRELATION_COEFFICIENT;
    }

    @Override
    public boolean isDecomposable()
    {
        return false;
    }

    @Override
    public ScoreOutputGroup getScoreOutputGroup()
    {
        return ScoreOutputGroup.NONE;
    }

    @Override
    public boolean hasRealUnits()
    {
        return false;
    }

    @Override
    public DoubleScoreOutput aggregate( DoubleScoreOutput output )
    {
        if ( Objects.isNull( output ) )
        {
            throw new MetricInputException( "Specify non-null input to the '" + this + "'." );
        }
        return output;
    }

    @Override
    public DoubleScoreOutput getInputForAggregation( SingleValuedPairs input )
    {
        return apply( input );
    }

    @Override
    public MetricConstants getCollectionOf()
    {
        return MetricConstants.PEARSON_CORRELATION_COEFFICIENT;
    }

    /**
     * Hidden constructor.
     */

    CorrelationPearsons()
    {
        super();
        correlation = new PearsonsCorrelation();
    }

}
