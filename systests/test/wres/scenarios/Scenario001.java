package wres.scenarios;

import org.junit.Test;
import static org.junit.Assert.*;
import java.nio.file.*;

public class Scenario001 {
    private AllScenarios scenario001 = null;
    
    public Scenario001() {
        setScenario001();
    }
    
    public void setScenario001() {
        scenario001 = new AllScenarios();
    }
    public void unsetScenario001() {
        scenario001 = null;
    }
    
    @Test 
    public void runtest() {
        //setScenario001();
        runScenario001();
    }
    
    public void runScenario001() {    
        Path tmppath = scenario001.runTest("scenario001");
        if (tmppath != null)
            scenario001.fileComparison( tmppath );
    }
}
