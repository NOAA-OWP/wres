package gov.noaa.wres.metric;

import java.util.function.ToDoubleFunction;
import gov.noaa.wres.datamodel.PairEvent;

public class SimpleError implements ToDoubleFunction<PairEvent>
{
    public double applyAsDouble(PairEvent pair)
    {
        return pair.getForecast() - pair.getObservation();
    }
}
