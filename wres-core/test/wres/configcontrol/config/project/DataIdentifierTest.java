package wres.configcontrol.config.project;

import java.net.URI;
import java.util.Map;
import java.util.TreeMap;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.testing.EqualsTester;

import wres.configcontrol.config.CompoundIdentifier;

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
        a.put(CompoundIdentifier.OBJECT_IDENTIFIER, "ORDC1");
        a.put(CompoundIdentifier.ATTRIBUTE_IDENTIFIER, "SQIN");
        a.put(CompoundIdentifier.SCENARIO_IDENTIFIER, "GEFS");

        final Map<Integer, String> b = new TreeMap<>();
        b.put(CompoundIdentifier.OBJECT_IDENTIFIER, "ORDC1");
        b.put(CompoundIdentifier.ATTRIBUTE_IDENTIFIER, "SQIN");
        b.put(CompoundIdentifier.SCENARIO_IDENTIFIER, "GEFS");

        final Map<Integer, String> c = new TreeMap<>();
        c.put(CompoundIdentifier.OBJECT_IDENTIFIER, "ORDC1");
        c.put(CompoundIdentifier.ATTRIBUTE_IDENTIFIER, "SQIN");

        final CompoundIdentifier id1 = new CompoundIdentifier(a);
        final CompoundIdentifier id2 = new CompoundIdentifier(b);
        final CompoundIdentifier id3 = new CompoundIdentifier(c);
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
        a.put(CompoundIdentifier.OBJECT_IDENTIFIER, "ORDC1");
        a.put(CompoundIdentifier.ATTRIBUTE_IDENTIFIER, "SQIN");
        a.put(CompoundIdentifier.SCENARIO_IDENTIFIER, "GEFS");

        final Map<Integer, String> b = new TreeMap<>();
        b.put(CompoundIdentifier.OBJECT_IDENTIFIER, "ORDC1");
        b.put(CompoundIdentifier.ATTRIBUTE_IDENTIFIER, "SQIN");
        b.put(CompoundIdentifier.SCENARIO_IDENTIFIER, "GEFS");

        final CompoundIdentifier id1 = new CompoundIdentifier(a);
        final CompoundIdentifier id2 = new CompoundIdentifier(b);
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
        c.put(CompoundIdentifier.OBJECT_IDENTIFIER, "ORDC1");
        c.put(CompoundIdentifier.ATTRIBUTE_IDENTIFIER, "SQIN");
        URI u3 = null;
        try
        {
            u3 = new URI("some.other.location");
        }
        catch(final Exception e)
        {
            Assert.fail("Test failed : " + e.getMessage());
        }
        final CompoundIdentifier id3 = new CompoundIdentifier(c);
        final DataIdentifier r = new DataIdentifier(id3, u3);
        final String msgB = "Comparison failed: <" + p + ">, <" + r + ">";
        Assert.assertTrue(msgB, p.compareTo(r) > 0);
        Assert.assertTrue(msgB, r.compareTo(p) < 0);
    }

}
