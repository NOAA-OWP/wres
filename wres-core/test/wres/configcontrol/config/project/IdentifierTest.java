package wres.configcontrol.config.project;

import java.util.Map;
import java.util.TreeMap;

import org.junit.Assert;
import org.junit.Test;

// Imports
import com.google.common.testing.EqualsTester;

// WRES dependencies
import wres.configcontrol.config.Identifier;

/**
 * Test class for {@link Identifier}
 * 
 * @author james.brown@hydrosolved.com
 */
public class IdentifierTest
{

    /**
     * Tests for equality of two {@link Identifier}.
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
        //Test
        new EqualsTester().addEqualityGroup(id1, id2).addEqualityGroup(id3).testEquals();
    }

    /**
     * Tests the {@link Comparable#compareTo(Object)} methods of two {@link Identifier}.
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
        final String msgA = "Objects were not comparable: <" + a + ">, <" + b + ">";
        final Identifier id1 = new Identifier(a);
        final Identifier id2 = new Identifier(b);
        Assert.assertTrue(msgA, id1.compareTo(id2) == 0);
        Assert.assertTrue(msgA, id2.compareTo(id1) == 0);
        final Map<Integer, String> c = new TreeMap<>();
        c.put(Identifier.OBJECT_IDENTIFIER, "ORDC1");
        c.put(Identifier.ATTRIBUTE_IDENTIFIER, "SQIN");
        final String msgB = "Comparison failed: <" + b + ">, <" + c + ">";
        final Identifier id3 = new Identifier(c);
        Assert.assertTrue(msgB, id1.compareTo(id3) > 0);
        Assert.assertTrue(msgB, id3.compareTo(id1) < 0);
    }

}
