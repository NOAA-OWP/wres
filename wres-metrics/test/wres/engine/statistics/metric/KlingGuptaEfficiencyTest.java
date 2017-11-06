package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.time.Instant;

import org.junit.Test;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.outputs.MetricOutputMetadata;
import wres.datamodel.outputs.VectorOutput;
import wres.datamodel.time.ReferenceTime;
import wres.datamodel.time.TimeWindow;
import wres.engine.statistics.metric.KlingGuptaEfficiency.KlingGuptaEfficiencyBuilder;

/**
 * Tests the {@link KlingGuptaEfficiency}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class KlingGuptaEfficiencyTest
{

    /**
     * Constructs a {@link KlingGuptaEfficiency} and compares the actual result to the expected result. Also, checks 
     * the parameters.
     * @throws MetricParameterException if the metric construction fails 
     */

    @Test
    public void test1KlingGuptaEfficiency() throws MetricParameterException
    {
        //Obtain the factories
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = outF.getMetadataFactory();

        //Generate some data
        SingleValuedPairs input = null;
        try
        {
            input = MetricTestDataFactory.getSingleValuedPairsFive();
        }
        catch ( IOException e )
        {
            fail( "Unable to read the test data." );
        }
        //Metadata for the output
        final TimeWindow window = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                 Instant.parse( "2010-12-31T11:59:59Z" ),
                                                 ReferenceTime.VALID_TIME,
                                                 24 );
        final MetricOutputMetadata m1 = metaFac.getOutputMetadata( input.getData().size(),
                                                                   metaFac.getDimension(),
                                                                   metaFac.getDimension( "MM/DAY" ),
                                                                   MetricConstants.KLING_GUPTA_EFFICIENCY,
                                                                   MetricConstants.NONE,
                                                                   metaFac.getDatasetIdentifier( "103.1",
                                                                                                 "QME",
                                                                                                 "NVE" ),
                                                                   window );

        //Build the metric
        final KlingGuptaEfficiencyBuilder b = new KlingGuptaEfficiencyBuilder();
        b.setOutputFactory( outF );
        final KlingGuptaEfficiency kge = b.build();

        //Check the parameters
        assertTrue( "Unexpected name for the Kling Gupta Efficiency.",
                    kge.getName().equals( metaFac.getMetricName( MetricConstants.KLING_GUPTA_EFFICIENCY ) ) );
        assertTrue( "The Kling Gupta Efficiency is decomposable.", kge.isDecomposable() );
        assertTrue( "The Kling Gupta Efficiency is a skill score.", kge.isSkillScore() );
        assertTrue( "Expected no decomposition for the Kling Gupta Efficiency.",
                    kge.getScoreOutputGroup() == ScoreOutputGroup.NONE );

        //Check the results
        final VectorOutput actual = kge.apply( input );

        final VectorOutput expected = outF.ofVectorOutput( new double[] { 0.8921704394462281 }, m1 );
        assertTrue( "Actual: " + actual.getData().getDoubles()[0]
                    + ". Expected: "
                    + expected.getData().getDoubles()[0]
                    + ".",
                    actual.equals( expected ) );
    }

    /**
     * Constructs a {@link KlingGuptaEfficiency} and checks for exceptional cases.
     * @throws MetricParameterException if the metric construction fails
     */

    @Test
    public void test2Exceptions() throws MetricParameterException
    {
        //Build the metric
        final DataFactory outF = DefaultDataFactory.getInstance();
        final KlingGuptaEfficiencyBuilder b = new KlingGuptaEfficiencyBuilder();
        b.setOutputFactory( outF );
        final KlingGuptaEfficiency kge = b.build();

        //Check the exceptions
        try
        {
            kge.apply( null );
            fail( "Expected an exception on null input." );
        }
        catch ( MetricInputException e )
        {
        }
    }

}
