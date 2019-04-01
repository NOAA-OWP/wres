package wres.io.data.caching;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import wres.io.utilities.TestDatabase;

@Ignore
public class UnitConversionTest
{
    private static final double EPSILON = 0.000001;

    private static TestDatabase testDatabase;
    private static ComboPooledDataSource dataSource;

    @BeforeClass
    public void setup()
    {
        UnitConversionTest.testDatabase = new TestDatabase( this.getClass()
                                                                .getSimpleName() );
        UnitConversionTest.dataSource = UnitConversionTest.testDatabase.getNewComboPooledDataSource();
    }

    @Test
    public void convertTemperatures()
    {
        // TODO: Write Temperature Conversion tests
        final double initial = 3.587;

        // convert C to F
        // check if F to C matches initial

        // Convert F to C
        // check if C to F matches initial

        // Convert F to K
        // check if K to F matches initial

        // Convert C to K
        // check if K to C matches initial
    }

    @Test
    public void convertCFS()
    {
        // TODO: Write Cubic Feet per Second tests
        final double initial = 3.587;

        // Convert CFS to CMS
        // check if CMS to CFS matches initial

        // Convert CFS to ft3/s
        // check if ft3/s to CFS matches initial

        // Convert CFS to KCFS
        // check if KCFS to CFS matches initial

        // Convert CFS to m3/s
        // check if m3/s to CFS matches initial

        // convert CFS to m3 s-1
        // check if m3 s-1 to CFS matches initial

        // Convert ft3/s to KCFS
        // Check if KCFS to ft3/s matches initial

        // Convert ft3/s to CMS
        // Check if CMS to ft3/s matches initial

        // Convert ft3/s to m3/s
        // Check if m3/s to ft3/s matches initial

        // Convert ft3/s to m3 s-1
        // Check if m3 s-1 to ft3/s matches initial

        // Convert KCFS to CMS
        // Check if CMS to KCFS matches initial

        // Convert KCFS to m3/s
        // Check if m3/s to KCFS matches initial

        // Convert KCFS to m3 s-1
        // Check if m3 s-1 to KCFS matches initial
    }

    @Test
    public void convertDistance()
    {
        // TODO: Write distance conversion tests
        final double initial = 3.587;
    }

    @Test
    public void convertCMS()
    {
        // TODO: Write Cubic meters per second tests
        final double initial = 3.587;
    }

    @Test
    public void convertkgm2()
    {
        // TODO: Write KGm2 Conversion tests
        final double initial = 3.587;
    }

    @Test
    public void convertmmTime()
    {
        // TODO: Write mm to time conversion tests
        final double initial = 3.587;
    }

    @AfterClass
    public void tearDown()
    {
        UnitConversionTest.dataSource.close();
        UnitConversionTest.dataSource = null;
        UnitConversionTest.testDatabase = null;
    }

    private boolean areEqual(double valueOne, double valueTwo)
    {
        return Math.abs(valueOne - valueTwo) < EPSILON;
    }
}
