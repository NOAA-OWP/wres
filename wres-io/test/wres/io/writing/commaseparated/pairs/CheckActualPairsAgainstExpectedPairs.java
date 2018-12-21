package wres.io.writing.commaseparated.pairs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Reads actual pairs and compares them to expected pairs.
 * 
 * Do NOT run in production.
 */

@Ignore
public final class CheckActualPairsAgainstExpectedPairs
{

    /**
     * Do NOT run in production.
     * 
     * Compares actual pairs against expected pairs in an unconditional way.
     *
     * This can be used to test actual against expected outputs under all circumstances.
     *
     * @throws IOException if the writing or removal of the paired file fails
     */

    @Test
    public void testActualPairsEqualExpectedPairsWithoutConditions() throws IOException
    {
        // To edit per test
        String scenarioNumber = "703";
        String actualOutputNumber = "1393927626314883771";

        // Expected pairs
        Path expectedPairs = Paths.get( "../systests/scenario" + scenarioNumber + "/benchmarks/", "sorted_pairs.csv" );
        assertTrue( expectedPairs.toFile().canRead() );

        // Actual pairs
        Path actualPairs = Paths.get( "some/path" + scenarioNumber
                                      + "/outputs/wres_evaluation_output_"
                                      + actualOutputNumber,
                                      "pairs.csv" );

        // Readable
        assertTrue( actualPairs.toFile().canRead() );

        List<String> actualRows = Files.readAllLines( actualPairs );
        List<String> expectedRows = Files.readAllLines( expectedPairs );

        // Some data read for both
        assertTrue( actualRows.size() > 0 && expectedRows.size() > 0 );

        // Same number of rows
        assertEquals( actualRows.size(), expectedRows.size() );

        // Sort in natural order
        Collections.sort( actualRows );
        Collections.sort( expectedRows );

        // Verify by row, rather than all at once        
        for ( int i = 0; i < actualRows.size(); i++ )
        {
            assertEquals( "Row " + i + " contained a difference.",
                          actualRows.get( i ),
                          expectedRows.get( i ) );
        }
    }

    /**
     * Do NOT run in production.
     * 
     * Compares actual pairs against expected pairs in a conditional way. Specifically,
     * this is intended to confirm the pairs locally produced by the fix to #55231
     * against the existing benchmarks prior to #55231. 
     *
     * @throws IOException if the writing or removal of the paired file fails
     */

    @Test
    public void testActualPairsEqualExpectedPairsWithConditions() throws IOException
    {
        // To edit per test
        String scenarioNumber = "001";  // The system test scenario number
        String actualOutputNumber = "6033086960545198915"; //The temp directory name assigned by WRES
        
        // Expected pairs
        Path expectedPairs = Paths.get( "../systests/scenario"+scenarioNumber+"/benchmarks/", "sorted_pairs.csv" );
        assertTrue( expectedPairs.toFile().canRead() );
        
        // Actual pairs
        Path actualPairs = Paths.get( "some/path" + scenarioNumber
                               + "/more/path/wres_evaluation_output_"+actualOutputNumber,
                               "pairs.csv" );
        
        // Readable
        assertTrue( actualPairs.toFile().canRead() );
        
        List<String> actualRows = Files.readAllLines( actualPairs );
        List<String> expectedRows = Files.readAllLines( expectedPairs );
        
        // Some data read for both
        assertTrue( actualRows.size() > 0 && expectedRows.size() > 0 );
        
        // Same number of rows
        assertEquals( actualRows.size(), expectedRows.size() );
        
        // Compare row-by-row, building from tokens to account for expected differences
        // ignore header
        // Store them for subsequent comparison because they are not ordered yet
        List<String> actualForComparison = new ArrayList<>();
        List<String> expectedForComparison = new ArrayList<>();
        
        for( int i = 0; i < actualRows.size(); i++ )
        {
            String actualRow = actualRows.get( i );
            String expectedRow = expectedRows.get( i );
            
            if( ! actualRow.startsWith( "FEATURE DESCRIPTION" ) ) 
            {
                String[] actualSplit = actualRow.split( "," );
                
                // Sort ensemble members in ascending OOM
                Arrays.sort( actualSplit, 10, actualSplit.length );
                
                // Eliminate new columns
                actualForComparison.add( String.join( ",", ArrayUtils.removeAll( actualSplit, 1,2,3,4,5,6 ) ) );

            }
            
            if( ! expectedRow.startsWith( "FEATURE DESCRIPTION" ) ) 
            {
                String[] expectedSplit = expectedRow.split( "," );
                
                // Sort ensemble members in ascending OOM
                Arrays.sort( expectedSplit, 5, expectedSplit.length );
                
                // Eliminate the time window index
                expectedForComparison.add( String.join( ",", ArrayUtils.remove( expectedSplit, 3 ) ) );
            }
        }

        // Sort in natural order
        Collections.sort( actualForComparison );
        Collections.sort( expectedForComparison );
        
        // Verify by row, rather than all at once        
        for ( int i = 0; i < actualForComparison.size(); i++ )
        {
            assertEquals( "Row " + i + " contained a difference.",
                          actualForComparison.get( i ),
                          expectedForComparison.get( i ) );
        }
    }


}
