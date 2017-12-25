package wres.engine.statistics.metric.singlevalued;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

import org.junit.Test;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.ReferenceTime;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.ScalarOutput;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.engine.statistics.metric.singlevalued.IndexOfAgreement.IndexOfAgreementBuilder;

/**
 * Tests the {@link IndexOfAgreement}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class IndexOfAgreementTest
{

    /**
     * Constructs a {@link IndexOfAgreement} and compares the actual result to the expected result. Also, checks the
     * parameters.
     * @throws MetricParameterException if the metric could not be constructed 
     */

    @Test
    public void test1IndexOfAgreement() throws MetricParameterException
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
                                                 Duration.ofHours( 24 ) );
        final MetricOutputMetadata m1 = metaFac.getOutputMetadata( input.getData().size(),
                                                                   metaFac.getDimension(),
                                                                   metaFac.getDimension( "MM/DAY" ),
                                                                   MetricConstants.INDEX_OF_AGREEMENT,
                                                                   MetricConstants.MAIN,
                                                                   metaFac.getDatasetIdentifier( "103.1",
                                                                                                 "QME",
                                                                                                 "NVE" ),
                                                                   window );

        //Build the metric
        final IndexOfAgreementBuilder b = new IndexOfAgreementBuilder();
        b.setOutputFactory( outF );
        final IndexOfAgreement ioa = b.build();

        //Check the parameters
        assertTrue( "Unexpected name for the Index of Agreement.",
                    ioa.getName().equals( MetricConstants.INDEX_OF_AGREEMENT.toString() ) );
        assertTrue( "The Index of Agreement is not decomposable.", !ioa.isDecomposable() );
        assertTrue( "The Index of Agreement is a skill score.", ioa.isSkillScore() );
        assertTrue( "Expected no decomposition for the Index of Agreement.",
                    ioa.getScoreOutputGroup() == ScoreOutputGroup.NONE );

        //Check the results
        final ScalarOutput actual = ioa.apply( input );

        final ScalarOutput expected = outF.ofScalarOutput( 0.8221179993380173, m1 );
        assertTrue( "Actual: " + actual.getData() + ". Expected: " + expected.getData() + ".",
                    actual.equals( expected ) );
    }

    /**
     * Constructs a {@link IndexOfAgreement} and checks for exceptional cases.
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void test2Exceptions() throws MetricParameterException
    {
        //Build the metric
        final DataFactory outF = DefaultDataFactory.getInstance();
        final IndexOfAgreementBuilder b = new IndexOfAgreementBuilder();
        b.setOutputFactory( outF );
        final IndexOfAgreement ioa = b.build();

        //Check the exceptions
        try
        {
            ioa.apply( null );
            fail( "Expected an exception on null input." );
        }
        catch ( MetricInputException e )
        {
        }
    }     
    
}
