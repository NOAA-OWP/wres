package gov.noaa.wres.metric;

import java.util.Arrays;
import java.util.function.Function;
import gov.noaa.wres.datamodel.PairEvent;

public class SimpleError implements Function<PairEvent,double[]>
{
    public double[] apply(PairEvent pair)
    {
        return Arrays.stream(pair.getForecasts())
            .map(forecastValue -> forecastValue - pair.getObservation())
            .toArray();

        /* old-fashioned, simple way
        return pair.getForecast() - pair.getObservation();
        */
    }
}
