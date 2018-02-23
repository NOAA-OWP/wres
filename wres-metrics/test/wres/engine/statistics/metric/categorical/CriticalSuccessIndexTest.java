package wres.engine.statistics.metric.categorical;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.inputs.pairs.DichotomousPairs;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.engine.statistics.metric.MetricFactory;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.engine.statistics.metric.categorical.ThreatScore.CriticalSuccessIndexBuilder;

/**
 * Tests the {@link ThreatScore}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class CriticalSuccessIndexTest
{

    /**
     * Constructs a {@link ThreatScore} and compares the actual result to the expected result. Also, checks the
     * parameters of the metric.
     * @throws MetricParameterException if the metric construction fails
     */

    @Test
    public void test1CriticalSuccessIndex() throws MetricParameterException
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
                                           MetricConstants.THREAT_SCORE,
                                           MetricConstants.MAIN,
                                           metaFac.getDatasetIdentifier( "DRRC2", "SQIN", "HEFS" ) );

        //Build the metric
        final CriticalSuccessIndexBuilder b = new ThreatScore.CriticalSuccessIndexBuilder();
        b.setOutputFactory( outF );
        final ThreatScore csi = b.build();

        //Check the results
        final DoubleScoreOutput actual = csi.apply( input );
        final MetricFactory metF = MetricFactory.getInstance( outF );
        final DoubleScoreOutput expected = outF.ofDoubleScoreOutput( 0.5734265734265734, m1 );
        assertTrue( "Actual: " + actual.getData().doubleValue()
                    + ". Expected: "
                    + expected.getData().doubleValue()
                    + ".",
                    actual.equals( expected ) );

        //Check the parameters
        assertTrue( "Unexpected name for the Critical Success Index.",
                    csi.getName().equals( MetricConstants.THREAT_SCORE.toString() ) );
        assertTrue( "The Critical Success Index is not decomposable.", !csi.isDecomposable() );
        assertTrue( "The Critical Success Index is not a skill score.", !csi.isSkillScore() );
        assertTrue( "The Critical Success Index cannot be decomposed.",
                    csi.getScoreOutputGroup() == ScoreOutputGroup.NONE );
        final String expName = metF.ofDichotomousContingencyTable().getName();
        final String actName = csi.getCollectionOf().toString();
        assertTrue( "The Critical Success Index should be a collection of '" + expName
                    + "', but is actually a collection of '"
                    + actName
                    + "'.",
                    csi.getCollectionOf() == metF.ofDichotomousContingencyTable().getID() );
    }

}
