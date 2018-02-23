package wres.engine.statistics.metric.categorical;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;

import org.junit.Test;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.DichotomousPairs;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.MatrixOutput;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.engine.statistics.metric.categorical.ContingencyTable.ContingencyTableBuilder;

/**
 * Tests the {@link ContingencyTable}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class ContingencyTableTest
{

    /**
     * Constructs a {@link ContingencyTable} and compares the actual result to the expected result. Also, checks the
     * parameters of the metric.
     * @throws MetricParameterException if the metric could not be constructed 
     */

    @Test
    public void test1ContingencyTable() throws MetricParameterException
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
                                           MetricConstants.CONTINGENCY_TABLE,
                                           MetricConstants.MAIN,
                                           metaFac.getDatasetIdentifier( "DRRC2", "SQIN", "HEFS" ) );

        //Build the metric
        final ContingencyTableBuilder<DichotomousPairs> b = new ContingencyTable.ContingencyTableBuilder<>();
        b.setOutputFactory( outF );
        final ContingencyTable<DichotomousPairs> table = b.build();
        final double[][] benchmark = new double[][] { { 82.0, 38.0 }, { 23.0, 222.0 } };
        final MatrixOutput actual = table.apply( input );
        final MatrixOutput expected = outF.ofMatrixOutput( benchmark,
                                                           Arrays.asList( MetricDimension.TRUE_POSITIVES,
                                                                          MetricDimension.FALSE_POSITIVES,
                                                                          MetricDimension.FALSE_NEGATIVES,
                                                                          MetricDimension.TRUE_NEGATIVES ),
                                                           m1 );       
        assertTrue( "Unexpected result for the contingency table.", actual.equals( expected ) );
        //Check the parameters
        assertTrue( table.getName().equals( MetricConstants.CONTINGENCY_TABLE.toString() ) );
    }

    /**
     * Constructs a {@link ContingencyTable} and checks for exceptional cases.
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void test2Exceptions() throws MetricParameterException
    {
        //Build the metric
        final DataFactory outF = DefaultDataFactory.getInstance();
        final ContingencyTableBuilder<DichotomousPairs> b = new ContingencyTable.ContingencyTableBuilder<>();
        b.setOutputFactory( outF );
        final ContingencyTable<DichotomousPairs> table = b.build();

        //Check the exceptions
        try
        {
            table.apply( (DichotomousPairs) null );
            fail( "Expected an exception on null input." );
        }
        catch ( MetricInputException e )
        {
        }
    }

}
