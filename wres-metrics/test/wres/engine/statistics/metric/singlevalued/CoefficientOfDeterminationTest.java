package wres.engine.statistics.metric.singlevalued;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.engine.statistics.metric.MetricCalculationException;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.engine.statistics.metric.singlevalued.CoefficientOfDetermination.CoefficientOfDeterminationBuilder;

/**
 * Tests the {@link CoefficientOfDetermination}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class CoefficientOfDeterminationTest
{

    /**
     * Constructs a {@link CoefficientOfDetermination}.
     * @throws MetricParameterException if the metric could not be constructed 
     */

    @Test
    public void test1CoefficientOfDetermination() throws MetricParameterException
    {
        //Obtain the factories
        final DataFactory dataF = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = dataF.getMetadataFactory();

        SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Build the metric
        final CoefficientOfDeterminationBuilder b = new CoefficientOfDetermination.CoefficientOfDeterminationBuilder();
        b.setOutputFactory( dataF );
        final CoefficientOfDetermination cod = b.build();

        final MetricOutputMetadata m1 = metaFac.getOutputMetadata( input.getData().size(),
                                                                   metaFac.getDimension(),
                                                                   metaFac.getDimension(),
                                                                   MetricConstants.COEFFICIENT_OF_DETERMINATION,
                                                                   MetricConstants.MAIN );

        //Compute normally
        final DoubleScoreOutput actual = cod.apply( input );
        final DoubleScoreOutput expected = dataF.ofDoubleScoreOutput( Math.pow( 0.9999999910148981, 2 ), m1 );
        assertTrue( "Actual: " + actual.getData().doubleValue()
                    + ". Expected: "
                    + expected.getData().doubleValue()
                    + ".",
                    actual.equals( expected ) );

        //Check the parameters
        assertTrue( "Unexpected name for coefficient of determination.",
                    cod.getName().equals( MetricConstants.COEFFICIENT_OF_DETERMINATION.toString() ) );
        assertTrue( "Coefficient of determination is not decomposable.", !cod.isDecomposable() );
        assertTrue( "Coefficient of determination is not a skill score.", !cod.isSkillScore() );
        assertTrue( "Coefficient of determination cannot be decomposed.",
                    cod.getScoreOutputGroup() == ScoreOutputGroup.NONE );
        assertTrue( "Coefficient of determination does not have real units", !cod.hasRealUnits() );
    }

    /**
     * Constructs a {@link CoefficientOfDetermination} and checks for exceptional cases.
     * @throws MetricParameterException if the metric could not be constructed 
     */

    @Test
    public void test2Exceptions() throws MetricParameterException
    {
        //Build the metric
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = outF.getMetadataFactory();
        final CoefficientOfDeterminationBuilder b = new CoefficientOfDetermination.CoefficientOfDeterminationBuilder();
        b.setOutputFactory( outF );
        final CoefficientOfDetermination cod = b.build();

        //Check the exceptions
        try
        {
            cod.apply( (SingleValuedPairs) null );
            fail( "Expected an exception on null input." );
        }
        catch ( MetricInputException e )
        {
        }
        try
        {
            List<PairOfDoubles> list = new ArrayList<>();
            list.add( outF.pairOf( 0.0, 0.0 ) );
            cod.apply( outF.ofSingleValuedPairs( list, metaFac.getOutputMetadata( 1,
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
