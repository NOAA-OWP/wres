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
import wres.engine.statistics.metric.categorical.ProbabilityOfFalseDetection;
import wres.engine.statistics.metric.categorical.ProbabilityOfFalseDetection.ProbabilityOfFalseDetectionBuilder;

/**
 * Tests the {@link ProbabilityOfFalseDetection}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class ProbabilityOfFalseDetectionTest
{

    /**
     * Constructs a {@link ProbabilityOfFalseDetection} and compares the actual result to the expected result. Also,
     * checks the parameters of the metric.
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void test1ProbabilityOfDetection() throws MetricParameterException
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
                                           MetricConstants.PROBABILITY_OF_FALSE_DETECTION,
                                           MetricConstants.MAIN,
                                           metaFac.getDatasetIdentifier( "DRRC2", "SQIN", "HEFS" ) );

        //Build the metric
        final ProbabilityOfFalseDetectionBuilder b =
                new ProbabilityOfFalseDetection.ProbabilityOfFalseDetectionBuilder();
        b.setOutputFactory( outF );
        final ProbabilityOfFalseDetection pofd = b.build();

        //Check the results
        final ScalarOutput actual = pofd.apply( input );
        final MetricFactory metF = MetricFactory.getInstance( outF );
        final ScalarOutput expected = outF.ofScalarOutput( 0.14615384615384616, m1 );
        assertTrue( "Actual: " + actual.getData().doubleValue()
                    + ". Expected: "
                    + expected.getData().doubleValue()
                    + ".",
                    actual.equals( expected ) );
        //Check the parameters
        assertTrue( "Unexpected name for the Probability of False Detection.",
                    pofd.getName().equals( MetricConstants.PROBABILITY_OF_FALSE_DETECTION.toString() ) );
        assertTrue( "The Probability of False Detection is not decomposable.", !pofd.isDecomposable() );
        assertTrue( "The Probability of False Detection is not a skill score.", !pofd.isSkillScore() );
        assertTrue( "The Probability of False Detection cannot be decomposed.",
                    pofd.getScoreOutputGroup() == ScoreOutputGroup.NONE );
        final String expName = metF.ofContingencyTable().getName();
        final String actName = pofd.getCollectionOf().toString();
        assertTrue( "The Probability of False Detection should be a collection of '" + expName
                    + "', but is actually a collection of '"
                    + actName
                    + "'.",
                    pofd.getCollectionOf() == metF.ofContingencyTable().getID() );
    }

}
