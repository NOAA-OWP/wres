package wres.scenarios;

import org.junit.Test;
import static org.junit.Assert.*;
import java.nio.file.*;

public class Scenario003 {
    private AllScenarios scenario003 = null;
    
    public Scenario003() {
        setScenario003();
    }
    
    public void setScenario003() {
        scenario003 = new AllScenarios();
    }
    public void unsetScenario003() {
        scenario003 = null;
    }
    
    @Test public void runtest() {
        runScenario003();
    }
    
    public void runScenario003() {    
        Path tmppath = scenario003.runTest("scenario003");
        if (tmppath != null)
            scenario003.fileComparison( tmppath );
    }
}
