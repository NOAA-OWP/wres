package gov.noaa.wres.datamodel;

import gov.noaa.wres.datamodel.LocationSingleTimeManyNonRasterData;
import gov.noaa.wres.datamodel.PairFactory;

import org.junit.Test;
import static org.junit.Assert.*;

public class FactoryBindingTest
{
    @Test
    public void useDataFactoryTest()
    {
        LocationSingleTimeManyNonRasterData timeseries = 
            PairFactory.getLocationSingleTimeManyNonRasterData();
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
