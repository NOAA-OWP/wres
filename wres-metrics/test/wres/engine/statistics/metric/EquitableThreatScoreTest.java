package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.DichotomousPairs;
import wres.datamodel.MatrixOutput;
import wres.datamodel.MetadataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.MetricInputException;
import wres.datamodel.MetricOutputMetadata;
import wres.datamodel.ScalarOutput;
import wres.engine.statistics.metric.EquitableThreatScore.EquitableThreatScoreBuilder;

/**
 * Tests the {@link EquitableThreatScore}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class EquitableThreatScoreTest
{

    /**
     * Constructs a {@link EquitableThreatScore} and compares the actual result to the expected result. Also, checks the
     * parameters of the metric.
     */

    @Test
    public void test1EquitableThreatScore()
    {
        //Obtain the factories
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = outF.getMetadataFactory();

        //Generate some data
        final DichotomousPairs input = MetricTestDataFactory.getDichotomousPairsOne();

        //Metadata for the output
        final MetricOutputMetadata m1 =
                metaFac.getOutputMetadata( input.getData().size(),
                                           metaFac.getDimension(),
                                           metaFac.getDimension(),
                                           MetricConstants.EQUITABLE_THREAT_SCORE,
                                           MetricConstants.MAIN,
                                           metaFac.getDatasetIdentifier( "DRRC2", "SQIN", "HEFS" ) );

        //Build the metric
        final EquitableThreatScoreBuilder b = new EquitableThreatScore.EquitableThreatScoreBuilder();
        b.setOutputFactory( outF );
        final EquitableThreatScore ets = b.build();

        //Check the results
        final MetricFactory metF = MetricFactory.getInstance( outF );
        final ScalarOutput actual = ets.apply( input );
        final ScalarOutput expected = outF.ofScalarOutput( 0.43768152544513195, m1 );
        assertTrue( "Actual: " + actual.getData().doubleValue()
                    + ". Expected: "
                    + expected.getData().doubleValue()
                    + ".",
                    actual.equals( expected ) );

        //Check the parameters
        assertTrue( "Unexpected name for the Equitable Threat Score.",
                    ets.getName().equals( metaFac.getMetricName( MetricConstants.EQUITABLE_THREAT_SCORE ) ) );
        assertTrue( "The Equitable Threat Score is not decomposable.", !ets.isDecomposable() );
        assertTrue( "The Equitable Threat Score is a skill score.", ets.isSkillScore() );
        assertTrue( "The Equitable Threat Score cannot be decomposed.",
                    ets.getScoreOutputGroup() == ScoreOutputGroup.NONE );
        final String expName = metF.ofContingencyTable().getName();
        final String actName = metaFac.getMetricName( ets.getCollectionOf() );
        assertTrue( "The Equitable Threat Score should be a collection of '" + expName
                    + "', but is actually a collection of '"
                    + actName
                    + "'.",
                    ets.getCollectionOf() == metF.ofContingencyTable().getID() );
    }

    /**
     * Constructs a {@link EquitableThreatScore} and checks for exceptional cases.
     */

    @Test
    public void test2Exceptions()
    {
        //Build the metric
        final DataFactory outF = DefaultDataFactory.getInstance();
        final EquitableThreatScoreBuilder b = new EquitableThreatScore.EquitableThreatScoreBuilder();
        b.setOutputFactory( outF );
        final EquitableThreatScore ets = b.build();

        //Check the exceptions
        try
        {
            ets.apply( (MatrixOutput) null );
            fail( "Expected an exception on null input." );
        }
        catch ( MetricInputException e )
        {
        }
    }

}
