package wres.io.reading;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import org.junit.jupiter.api.Test;

/**
 * Tests the {@link ReaderUtilities}.
 * @author James Brown
 */

class ReaderUtilitiesTest
{
    @Test
    void testSplitByDelimiter()
    {
        String csvToSplit = "\"foo, bar\",\"baz\",qux,";
        String[] actual = ReaderUtilities.splitByDelimiter( csvToSplit, ',' );
        assertArrayEquals( new String[] { "foo, bar", "baz", "qux", "" }, actual );
    }
    
    @Test
    void testSplitByDelimiterIssue100674()
    {
        String csvToSplit = "\"Seboeis River near Shin Pond, Maine\"";
        String[] actual = ReaderUtilities.splitByDelimiter( csvToSplit, ',' );
        assertArrayEquals( new String[] { "Seboeis River near Shin Pond, Maine" }, actual );
    }
}
