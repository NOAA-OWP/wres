/**
 * 
 */
package wres.configcontrol.config.project;

import java.net.URI;
// Imports
import java.util.Map;
import java.util.TreeMap;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.testing.EqualsTester;

// WRES dependencies
import wres.configcontrol.config.Identifier;

/**
 * Test class for {@link DataIdentifier}
 * 
 * @author james.brown@hydrosolved.com
 */
public class DataIdentifierTest
{

    /**
     * Tests for equality of two {@link DataIdentifier}.
     */
    @Test
    public void assertEqual()
    {
        final Map<Integer, String> a = new TreeMap<>();
        a.put(Identifier.OBJECT_IDENTIFIER, "ORDC1");
        a.put(Identifier.ATTRIBUTE_IDENTIFIER, "SQIN");
        a.put(Identifier.SCENARIO_IDENTIFIER, "GEFS");

        final Map<Integer, String> b = new TreeMap<>();
        b.put(Identifier.OBJECT_IDENTIFIER, "ORDC1");
        b.put(Identifier.ATTRIBUTE_IDENTIFIER, "SQIN");
        b.put(Identifier.SCENARIO_IDENTIFIER, "GEFS");

        final Map<Integer, String> c = new TreeMap<>();
        c.put(Identifier.OBJECT_IDENTIFIER, "ORDC1");
        c.put(Identifier.ATTRIBUTE_IDENTIFIER, "SQIN");

        final Identifier id1 = new Identifier(a);
        final Identifier id2 = new Identifier(b);
        final Identifier id3 = new Identifier(c);
        URI u1 = null;
        URI u2 = null;
        URI u3 = null;
        try
        {
            u1 = new URI("some.location");
            u2 = new URI("some.location");
            u3 = new URI("some.other.location");
        }
        catch(final Exception e)
        {
            Assert.fail("Test failed : " + e.getMessage());
        }
        final DataIdentifier p = new DataIdentifier(id1, u1);
        final DataIdentifier q = new DataIdentifier(id2, u2);
        final DataIdentifier r = new DataIdentifier(id3, u3);
        //Test
        new EqualsTester().addEqualityGroup(p, q).addEqualityGroup(r).testEquals();
    }

    /**
     * Tests the {@link Comparable#compareTo(Object)} methods of two {@link DataIdentifier}.
     */
    @Test
    public void assertComparable()
    {
        final Map<Integer, String> a = new TreeMap<>();
        a.put(Identifier.OBJECT_IDENTIFIER, "ORDC1");
        a.put(Identifier.ATTRIBUTE_IDENTIFIER, "SQIN");
        a.put(Identifier.SCENARIO_IDENTIFIER, "GEFS");

        final Map<Integer, String> b = new TreeMap<>();
        b.put(Identifier.OBJECT_IDENTIFIER, "ORDC1");
        b.put(Identifier.ATTRIBUTE_IDENTIFIER, "SQIN");
        b.put(Identifier.SCENARIO_IDENTIFIER, "GEFS");

        final Identifier id1 = new Identifier(a);
        final Identifier id2 = new Identifier(b);
        URI u1 = null;
        URI u2 = null;
        try
        {
            u1 = new URI("some.location");
            u2 = new URI("some.location");
        }
        catch(final Exception e)
        {
            Assert.fail("Test failed : " + e.getMessage());
        }
        final DataIdentifier p = new DataIdentifier(id1, u1);
        final DataIdentifier q = new DataIdentifier(id2, u2);
        final String msgA = "Objects were not comparable: <" + p + ">, <" + q + ">";

        Assert.assertTrue(msgA, p.compareTo(q) == 0);
        Assert.assertTrue(msgA, q.compareTo(p) == 0);

        final Map<Integer, String> c = new TreeMap<>();
        c.put(Identifier.OBJECT_IDENTIFIER, "ORDC1");
        c.put(Identifier.ATTRIBUTE_IDENTIFIER, "SQIN");
        URI u3 = null;
        try
        {
            u3 = new URI("some.other.location");
        }
        catch(final Exception e)
        {
            Assert.fail("Test failed : " + e.getMessage());
        }
        final Identifier id3 = new Identifier(c);
        final DataIdentifier r = new DataIdentifier(id3, u3);
        final String msgB = "Comparison failed: <" + p + ">, <" + r + ">";
        Assert.assertTrue(msgB, p.compareTo(r) > 0);
        Assert.assertTrue(msgB, r.compareTo(p) < 0);
    }

}
