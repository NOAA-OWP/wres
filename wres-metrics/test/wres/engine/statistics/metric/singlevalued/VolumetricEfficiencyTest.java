package wres.engine.statistics.metric.singlevalued;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.DiscreteProbabilityPairs;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.ReferenceTime;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.engine.statistics.metric.singlevalued.VolumetricEfficiency.VolumetricEfficiencyBuilder;

/**
 * Tests the {@link VolumetricEfficiency}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class VolumetricEfficiencyTest
{
    
    @Rule
    public final ExpectedException exception = ExpectedException.none();
    
    /**
     * Default instance of a {@link VolumetricEfficiency}.
     */

    private VolumetricEfficiency ve;

    /**
     * Instance of a data factory.
     */

    private DataFactory outF;

    @Before
    public void setupBeforeEachTest() throws MetricParameterException
    {
        VolumetricEfficiencyBuilder b = new VolumetricEfficiency.VolumetricEfficiencyBuilder();
        this.outF = DefaultDataFactory.getInstance();
        b.setOutputFactory( outF );
        this.ve = b.build();
    }

    /**
     * Compares the output from {@link VolumetricEfficiency#apply(SingleValuedPairs)} against expected output.
     * @throws IOException if the input data could not be read
     */

    @Test
    public void testApply() throws IOException
    {
        //Generate some data
        SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsFive();

        //Metadata for the output
        MetadataFactory metaFac = outF.getMetadataFactory();
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
        //Check the results
        DoubleScoreOutput actual = ve.apply( input );
        DoubleScoreOutput expected = outF.ofDoubleScoreOutput( 0.657420176533252, m1 );
        assertTrue( "Actual: " + actual.getData().doubleValue()
                    + ". Expected: "
                    + expected.getData().doubleValue()
                    + ".",
                    actual.equals( expected ) );
    }

    /**
     * Validates the output from {@link VolumetricEfficiency#apply(SingleValuedPairs)} when supplied with no data.
     */

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        DiscreteProbabilityPairs input =
                outF.ofDiscreteProbabilityPairs( Arrays.asList(), outF.getMetadataFactory().getMetadata() );
 
        DoubleScoreOutput actual = ve.apply( input );

        assertTrue( actual.getData().isNaN() );
    }

    /**
     * Checks that the {@link VolumetricEfficiency#getName()} returns 
     * {@link MetricConstants#VOLUMETRIC_EFFICIENCY.toString()}
     */

    @Test
    public void testGetName()
    {
        assertTrue( ve.getName().equals( MetricConstants.VOLUMETRIC_EFFICIENCY.toString() ) );
    }

    /**
     * Checks that the {@link VolumetricEfficiency#isDecomposable()} returns <code>false</code>.
     */

    @Test
    public void testIsDecomposable()
    {
        assertFalse( ve.isDecomposable() );
    }

    /**
     * Checks that the {@link VolumetricEfficiency#isSkillScore()} returns <code>false</code>.
     */

    @Test
    public void testIsSkillScore()
    {
        assertFalse( ve.isSkillScore() );
    }

    /**
     * Checks that the {@link VolumetricEfficiency#getScoreOutputGroup()} returns the result provided on construction.
     */

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( ve.getScoreOutputGroup() == ScoreOutputGroup.NONE );
    }

    /**
     * Tests for an expected exception on calling {@link VolumetricEfficiency#apply(SingleValuedPairs)} with null 
     * input.
     */

    @Test
    public void testApplyExceptionOnNullInput()
    {
        exception.expect( MetricInputException.class );
        exception.expectMessage( "Specify non-null input to the 'VOLUMETRIC EFFICIENCY'." );
        
        ve.apply( null );
    }
    
}
