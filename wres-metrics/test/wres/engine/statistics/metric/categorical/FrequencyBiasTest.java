package wres.engine.statistics.metric.categorical;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.inputs.pairs.DichotomousPairs;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.outputs.MetricOutputMetadata;
import wres.datamodel.outputs.ScalarOutput;
import wres.engine.statistics.metric.MetricFactory;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.engine.statistics.metric.categorical.FrequencyBias;
import wres.engine.statistics.metric.categorical.FrequencyBias.FrequencyBiasBuilder;

/**
 * Tests the {@link FrequencyBias}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class FrequencyBiasTest
{

    /**
     * Constructs a {@link FrequencyBias} and compares the actual result to the expected result. Also, checks the
     * parameters of the metric.
     * @throws MetricParameterException if the metric construction fails 
     */

    @Test
    public void test1FrequencyBias() throws MetricParameterException
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
                                           MetricConstants.FREQUENCY_BIAS,
                                           MetricConstants.MAIN,
                                           metaFac.getDatasetIdentifier( "DRRC2", "SQIN", "HEFS" ) );

        //Build the metric
        final FrequencyBiasBuilder b = new FrequencyBiasBuilder();
        b.setOutputFactory( outF );
        final FrequencyBias fb = b.build();

        //Check the results
        final MetricFactory metF = MetricFactory.getInstance( outF );
        final ScalarOutput actual = fb.apply( input );
        final ScalarOutput expected = outF.ofScalarOutput( 1.1428571428571428, m1 );
        assertTrue( "Actual: " + actual.getData().doubleValue()
                    + ". Expected: "
                    + expected.getData().doubleValue()
                    + ".",
                    actual.equals( expected ) );
        //Check the parameters
        assertTrue( "Unexpected name for the Frequency Bias.",
                    fb.getName().equals( MetricConstants.FREQUENCY_BIAS.toString() ) );
        assertTrue( "The Frequency Bias is not decomposable.", !fb.isDecomposable() );
        assertTrue( "The Frequency Bias is not a skill score.", !fb.isSkillScore() );
        assertTrue( "The Frequency Bias cannot be decomposed.",
                    fb.getScoreOutputGroup() == ScoreOutputGroup.NONE );
        final String expName = metF.ofContingencyTable().getName();
        final String actName = fb.getCollectionOf().toString();
        assertTrue( "The Frequency Bias should be a collection of '" + expName
                    + "', but is actually a collection of '"
                    + actName
                    + "'.",
                    fb.getCollectionOf() == metF.ofContingencyTable().getID() );

    }

}
