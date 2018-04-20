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
import wres.datamodel.outputs.ScoreOutput;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.engine.statistics.metric.singlevalued.VolumetricEfficiency.VolumetricEfficiencyBuilder;

/**
 * Tests the {@link VolumetricEfficiency}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class VolumetricEfficiencyTest
{

    /**
     * Constructs a {@link VolumetricEfficiency} and compares the actual result to the expected result. Also, checks 
     * the parameters.
     * @throws MetricParameterException if the metric could not be constructed 
     * @throws IOException if the test data could not be read
     */

    @Test
    public void test1VolumetricEfficiency() throws MetricParameterException, IOException
    {
        //Obtain the factories
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = outF.getMetadataFactory();

        //Generate some data
        SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsFive();

        //Metadata for the output
        final TimeWindow window = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                 Instant.parse( "2010-12-31T11:59:59Z" ),
                                                 ReferenceTime.VALID_TIME,
                                                 Duration.ofHours( 24 ) );
        final MetricOutputMetadata m1 = metaFac.getOutputMetadata( input.getRawData().size(),
                                                                   metaFac.getDimension(),
                                                                   metaFac.getDimension( "MM/DAY" ),
                                                                   MetricConstants.VOLUMETRIC_EFFICIENCY,
                                                                   MetricConstants.MAIN,
                                                                   metaFac.getDatasetIdentifier( "103.1",
                                                                                                 "QME",
                                                                                                 "NVE" ),
                                                                   window );

        //Build the metric
        final VolumetricEfficiencyBuilder b = new VolumetricEfficiencyBuilder();
        b.setOutputFactory( outF );
        final VolumetricEfficiency ve = b.build();

        //Check the parameters
        assertTrue( "Unexpected name for the Volumetric Efficiency.",
                    ve.getName().equals( MetricConstants.VOLUMETRIC_EFFICIENCY.toString() ) );
        assertTrue( "The Volumetric Efficiency is not decomposable.", !ve.isDecomposable() );
        assertTrue( "The Volumetric Efficiency is not a skill score.", !ve.isSkillScore() );
        assertTrue( "Expected no decomposition for the Volumetric Efficiency.",
                    ve.getScoreOutputGroup() == ScoreOutputGroup.NONE );

        //Check the results
        final ScoreOutput actual = ve.apply( input );

        final ScoreOutput expected = outF.ofDoubleScoreOutput( 0.657420176533252, m1 );
        assertTrue( "Actual: " + actual.getData() + ". Expected: " + expected.getData() + ".",
                    actual.equals( expected ) );
    }

    /**
     * Constructs a {@link VolumetricEfficiency} and checks for exceptional cases.
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void test2Exceptions() throws MetricParameterException
    {
        //Build the metric
        final DataFactory outF = DefaultDataFactory.getInstance();
        final VolumetricEfficiencyBuilder b = new VolumetricEfficiencyBuilder();
        b.setOutputFactory( outF );
        final VolumetricEfficiency ve = b.build();

        //Check the exceptions
        try
        {
            ve.apply( null );
            fail( "Expected an exception on null input." );
        }
        catch ( MetricInputException e )
        {
        }
    }     
    
}
