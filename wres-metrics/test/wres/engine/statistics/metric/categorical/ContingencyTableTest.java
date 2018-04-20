package wres.engine.statistics.metric.categorical;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.DichotomousPairs;
import wres.datamodel.inputs.pairs.MulticategoryPairs;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.MatrixOutput;
import wres.engine.statistics.metric.Metric;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.MetricTestDataFactory;

/**
 * Tests the {@link ContingencyTable}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class ContingencyTableTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Output factory.
     */

    private DataFactory outF;

    /**
     * Metadata factory.
     */

    private MetadataFactory metaFac;

    /**
     * The metric to test.
     */

    private ContingencyTable<MulticategoryPairs> table;

    @Before
    public void setupBeforeEachTest() throws MetricParameterException
    {
        outF = DefaultDataFactory.getInstance();
        metaFac = outF.getMetadataFactory();
        table = new ContingencyTable.ContingencyTableBuilder<>().setOutputFactory( outF ).build();
    }

    /**
     * Compares the output from {@link Metric#apply(wres.datamodel.inputs.MetricInput)} against expected output.
     */

    @Test
    public void testApply()
    {
        //Generate some data
        final DichotomousPairs input = MetricTestDataFactory.getDichotomousPairsOne();

        //Metadata for the output
        final MetricOutputMetadata meta =
                metaFac.getOutputMetadata( input.getRawData().size(),
                                           metaFac.getDimension(),
                                           metaFac.getDimension(),
                                           MetricConstants.CONTINGENCY_TABLE,
                                           MetricConstants.MAIN,
                                           metaFac.getDatasetIdentifier( "DRRC2", "SQIN", "HEFS" ) );

        final double[][] benchmark = new double[][] { { 82.0, 38.0 }, { 23.0, 222.0 } };
        final MatrixOutput actual = table.apply( input );
        final MatrixOutput expected = outF.ofMatrixOutput( benchmark,
                                                           Arrays.asList( MetricDimension.TRUE_POSITIVES,
                                                                          MetricDimension.FALSE_POSITIVES,
                                                                          MetricDimension.FALSE_NEGATIVES,
                                                                          MetricDimension.TRUE_NEGATIVES ),
                                                           meta );
        assertTrue( "Unexpected result for the contingency table.", actual.equals( expected ) );

        //Check the parameters
        assertTrue( table.getName().equals( MetricConstants.CONTINGENCY_TABLE.toString() ) );
    }

    /**
     * Checks that the {@link ContingencyTable#getName()} returns {@link MetricConstants.CONTINGENCY_TABLE.toString()}
     * @throws MetricParameterException if the metric could not be constructed 
     */

    @Test
    public void testContingencyTableIsNamedCorrectly() throws MetricParameterException
    {
        assertTrue( table.getName().equals( MetricConstants.CONTINGENCY_TABLE.toString() ) );
    }

    /**
     * Checks for an expected exception on null input to {@link ContingencyTable#apply(wres.datamodel.inputs.MetricInput)}.
     */

    @Test
    public void testExceptionOnNullInput()
    {
        exception.expect( MetricInputException.class );
        exception.expectMessage( "Specify non-null input to the 'CONTINGENCY TABLE'." );
        table.apply( (DichotomousPairs) null );
    }

    /**
     * Checks for an expected exception when building with a null builder.
     * @throws MetricParameterException if a different exceptional outcome than expected occurs
     */

    @Test
    public void testExceptionOnNullBuilder() throws MetricParameterException
    {
        exception.expect( MetricParameterException.class );
        exception.expectMessage( "Cannot construct the metric with a null builder." );
        new ContingencyTable<>( null );
    }    
    
    /**
     * Checks for an expected exception when building without a {@link DataFactory}.
     * @throws MetricParameterException if a different exceptional outcome than expected occurs
     */

    @Test
    public void testExceptionOnMissingDataFactory() throws MetricParameterException
    {
        exception.expect( MetricParameterException.class );
        exception.expectMessage( "Specify a data factory with which to build the metric." );
        new ContingencyTable.ContingencyTableBuilder<>().build();
    } 
    
}
