package wres.scenarios;

import org.junit.Test;
import static org.junit.Assert.*;



public class ScenarioTest {

    //private HelloTest helloTest = null;
    private Scenario001 scenario001 = null;
    private Scenario003 scenario003 = null;
    
    public ScenarioTest() {
        //setTest();
    }
    /*
    public void setTest() {
        helloTest = new HelloTest();
    }
    public void unsetTest() {
        helloTest = null;
    }
    */
    /*
    @Test public void testTheMain() {
        //public void testTheMain() {
            //Main testMain = new Main();
            assertNotNull("get version",
            "gjkdjgkd");
          //  new HelloTest();
           // testScenarios();
        }
    */
    @Test public void testScenarios000() {
    //public void testScenarios000() {    
        testScenario001();
        testScenario003();
        //testScenario007();
        //testScenario008();
        //testScenario009();
        
    }
    /*
    @Test public void testScenarios010() {
        //testScenario010();
    } */
    
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
    /*
    public void testScenario007() {
        helloTest.prepare4Testing("scenario007");
    }
    public void testScenario008() {
        helloTest.prepare4Testing("scenario008");           
    }
    public void testScenario009() {
        helloTest.prepare4Testing("scenario009");     
    }
    
    public void testScenario010() {
        helloTest.prepare4Testing("scenario010");     
    } 
    */
}


