package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.junit.Test;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetadataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDecompositionGroup;
import wres.datamodel.MetricOutputMetadata;
import wres.datamodel.ScalarOutput;
import wres.datamodel.SingleValuedPairs;
import wres.engine.statistics.metric.IndexOfAgreement.IndexOfAgreementBuilder;

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
     */

    @Test
    public void test1IndexOfAgreement()
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
        final MetricOutputMetadata m1 = metaFac.getOutputMetadata( input.getData().size(),
                                                                   metaFac.getDimension(),
                                                                   metaFac.getDimension( "MM/DAY" ),
                                                                   MetricConstants.INDEX_OF_AGREEMENT,
                                                                   MetricConstants.MAIN,
                                                                   metaFac.getDatasetIdentifier( "103.1",
                                                                                                 "QME",
                                                                                                 "NVE" ),
                                                                   24 );

        //Build the metric
        final IndexOfAgreementBuilder b = new IndexOfAgreementBuilder();
        b.setOutputFactory( outF );
        final IndexOfAgreement ioa = b.build();

        //Check the parameters
        assertTrue( "Unexpected name for the Index of Agreement.",
                    ioa.getName().equals( metaFac.getMetricName( MetricConstants.INDEX_OF_AGREEMENT ) ) );
        assertTrue( "The Index of Agreement is not decomposable.", !ioa.isDecomposable() );
        assertTrue( "The Index of Agreement is a skill score.", ioa.isSkillScore() );
        assertTrue( "Expected no decomposition for the Index of Agreement.",
                    ioa.getDecompositionID() == MetricDecompositionGroup.NONE );

        //Check the results
        final ScalarOutput actual = ioa.apply( input );

        final ScalarOutput expected = outF.ofScalarOutput( 0.8221179993380173, m1 );
        assertTrue( "Actual: " + actual.getData() + ". Expected: " + expected.getData() + ".",
                    actual.equals( expected ) );
    }

}
