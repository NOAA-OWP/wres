package wres.engine.statistics.metric.singlevalued;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.junit.Test;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.outputs.MetricOutputMetadata;
import wres.datamodel.outputs.ScalarOutput;
import wres.engine.statistics.metric.MetricCalculationException;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.engine.statistics.metric.singlevalued.CorrelationPearsons;
import wres.engine.statistics.metric.singlevalued.CorrelationPearsons.CorrelationPearsonsBuilder;

/**
 * Tests the {@link CorrelationPearsons}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class CorrelationPearsonsTest
{

    /**
     * Constructs a {@link CorrelationPearsons}. Minimal test that focuses on the wrapper, and not the underlying
     * {@link PearsonsCorrelation}.
     * @throws MetricParameterException if the metric could not be constructed 
     */

    @Test
    public void test1Correlation() throws MetricParameterException
    {
        //Obtain the factories
        final DataFactory dataF = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = dataF.getMetadataFactory();

        SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Build the metric
        final CorrelationPearsonsBuilder b = new CorrelationPearsons.CorrelationPearsonsBuilder();
        b.setOutputFactory( dataF );
        final CorrelationPearsons rho = b.build();

        final MetricOutputMetadata m1 = metaFac.getOutputMetadata( input.getData().size(),
                                                                   metaFac.getDimension(),
                                                                   metaFac.getDimension(),
                                                                   MetricConstants.PEARSON_CORRELATION_COEFFICIENT,
                                                                   MetricConstants.MAIN );

        //Compute normally
        final ScalarOutput actual = rho.apply( input );
        final ScalarOutput expected = dataF.ofScalarOutput( 0.9999999910148981, m1 );
        assertTrue( "Actual: " + actual.getData().doubleValue()
                    + ". Expected: "
                    + expected.getData().doubleValue()
                    + ".",
                    actual.equals( expected ) );

        //Check the parameters
        assertTrue( "Unexpected name for Pearson's correlation coefficient.",
                    rho.getName().equals( MetricConstants.PEARSON_CORRELATION_COEFFICIENT.toString() ) );
        assertTrue( "Pearson's correlation is not decomposable.", !rho.isDecomposable() );
        assertTrue( "Pearson's correlation is not a skill score.", !rho.isSkillScore() );
        assertTrue( "Pearson's correlation cannot be decomposed.",
                    rho.getScoreOutputGroup() == ScoreOutputGroup.NONE );
        assertTrue( "Pearson's correlation does not have real units", !rho.hasRealUnits() );
    }

    /**
     * Constructs a {@link CorrelationPearsons} and checks for exceptional cases.
     * @throws MetricParameterException if the metric could not be constructed 
     */

    @Test
    public void test2Exceptions() throws MetricParameterException
    {
        //Build the metric
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = outF.getMetadataFactory();
        final CorrelationPearsonsBuilder b = new CorrelationPearsons.CorrelationPearsonsBuilder();
        b.setOutputFactory( outF );
        final CorrelationPearsons rho = b.build();

        //Check the exceptions
        try
        {
            rho.apply( (SingleValuedPairs) null );
            fail( "Expected an exception on null input." );
        }
        catch ( MetricInputException e )
        {
        }
        try
        {
            List<PairOfDoubles> list = new ArrayList<>();
            list.add( outF.pairOf( 0.0, 0.0 ) );
            rho.apply( outF.ofSingleValuedPairs( list, metaFac.getOutputMetadata( 1,
                                                                                  metaFac.getDimension(),
                                                                                  metaFac.getDimension(),
                                                                                  MetricConstants.PEARSON_CORRELATION_COEFFICIENT ) ) );
            fail( "Expected a checked exception on invalid inputs: insufficient pairs." );
        }
        catch ( MetricCalculationException e )
        {
        }
    }

}
