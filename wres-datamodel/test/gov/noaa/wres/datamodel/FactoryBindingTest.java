package gov.noaa.wres.datamodel;

import gov.noaa.wres.datamodel.LocationSingleTimeManyNonRasterData;
import gov.noaa.wres.datamodel.DataFactory;

import org.junit.Test;
import static org.junit.Assert.*;

public class FactoryBindingTest
{
    @Test
    public void useDataFactoryTest()
    {
        LocationSingleTimeManyNonRasterData timeseries = 
            DataFactory.getLocationSingleTimeManyNonRasterData();
        assertNotNull(timeseries);
        assertNotNull(timeseries.getWresPoint());
        assertEquals(timeseries.getWresPoint().getX(), 0);
        assertEquals(timeseries.getWresPoint().getY(), Integer.MIN_VALUE);
        assertEquals(timeseries.getWresPoint().getZ(), Integer.MIN_VALUE);
        assertNotNull(timeseries.getDateTimes());
        assertNotNull(timeseries.getForecastValues("0"));
        assertNotNull(timeseries.getObservationValues());
    }
}
