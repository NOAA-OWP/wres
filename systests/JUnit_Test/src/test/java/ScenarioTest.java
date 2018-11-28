import org.junit.Test;
import static org.junit.Assert.*;
import java.nio.file.*;



public class ScenarioTest {

    @Test public void testScenarios000() {
    //public void testScenarios000() {    
    //    testScenario001();
    //    testScenario003();
    //    testScenario007();
    	testScenario008();
        //testScenario009();
        
    }
    /*
    @Test public void testScenarios010() {
        //testScenario010();
    } */
   /* 
    public void testScenario001() {
        scenario001 = new Scenario001();
        scenario001.runtest(); 
        //scenario001.unsetScenario001();;
    }
   
    public void testScenario003() {
        scenario003 = new Scenario003();
        scenario003.runtest();
        //scenario003.unsetScenario003();
    }
   */ 
/*
    public void testScenario007() {
	AllScenarios allScenarios = new AllScenarios();
        allScenarios.runTest("scenario007");
    }
*/
    public void testScenario008() {
        AllScenarios allScenarios = new AllScenarios();
        allScenarios.runTest("scenario008");
    }
/*
    public void testScenario009() {
        helloTest.prepare4Testing("scenario009");     
    }
    
    public void testScenario010() {
        helloTest.prepare4Testing("scenario010");     
    } 
    */
}


