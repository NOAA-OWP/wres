package wres.configcontrol.config.project;

//Imports
import com.google.common.testing.*;
import java.util.*;
import org.junit.*;

//WRES dependencies
import wres.configcontrol.config.*;

/**
 * Test class for {@link Identifier} 
 * 
 * @author james.brown@hydrosolved.com
 */
public class IdentifierTest {
	
	/**
	 * Tests for equality of two {@link Identifier}.
	 */	
	@Test
	public void assertEqual() {
		Map<Integer,String> a = new TreeMap<>();
		a.put(Identifier.OBJECT_IDENTIFIER,"ORDC1"); 
		a.put(Identifier.ATTRIBUTE_IDENTIFIER,"SQIN"); 
		a.put(Identifier.SCENARIO_IDENTIFIER,"GEFS"); 

		Map<Integer,String> b = new TreeMap<>();
		b.put(Identifier.OBJECT_IDENTIFIER,"ORDC1"); 
		b.put(Identifier.ATTRIBUTE_IDENTIFIER,"SQIN"); 
		b.put(Identifier.SCENARIO_IDENTIFIER,"GEFS"); 		

		Map<Integer,String> c = new TreeMap<>();
		c.put(Identifier.OBJECT_IDENTIFIER,"ORDC1"); 
		c.put(Identifier.ATTRIBUTE_IDENTIFIER,"SQIN"); 
		
		Identifier id1 = new Identifier(a);
		Identifier id2 = new Identifier(b);
		Identifier id3 = new Identifier(c);
		//Test
		new EqualsTester()
	     .addEqualityGroup(id1, id2)
	     .addEqualityGroup(id3)
	     .testEquals();
	}	
	
	/**
	 * Tests the {@link Comparable#compareTo(Object)} methods of two {@link Identifier}.
	 */	
	@Test
	public void assertComparable() {
		Map<Integer,String> a = new TreeMap<>();
		a.put(Identifier.OBJECT_IDENTIFIER,"ORDC1"); 
		a.put(Identifier.ATTRIBUTE_IDENTIFIER,"SQIN"); 
		a.put(Identifier.SCENARIO_IDENTIFIER,"GEFS"); 

		Map<Integer,String> b = new TreeMap<>();
		b.put(Identifier.OBJECT_IDENTIFIER,"ORDC1"); 
		b.put(Identifier.ATTRIBUTE_IDENTIFIER,"SQIN"); 
		b.put(Identifier.SCENARIO_IDENTIFIER,"GEFS");	    
		String msgA = "Objects were not comparable: <" + a + ">, <" + b + ">";
		Identifier id1 = new Identifier(a);
		Identifier id2 = new Identifier(b);
		Assert.assertTrue(msgA,id1.compareTo(id2)==0);
		Assert.assertTrue(msgA,id2.compareTo(id1)==0);
		Map<Integer,String> c = new TreeMap<>();
		c.put(Identifier.OBJECT_IDENTIFIER,"ORDC1"); 
		c.put(Identifier.ATTRIBUTE_IDENTIFIER,"SQIN"); 
		String msgB = "Comparison failed: <" + b + ">, <" + c + ">";
		Identifier id3 = new Identifier(c);
		Assert.assertTrue(msgB,id1.compareTo(id3)>0);
		Assert.assertTrue(msgB,id3.compareTo(id1)<0);
	}	
	

}
