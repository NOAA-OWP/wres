package wres.datamodel.time;

import java.time.Instant;
import java.util.SortedSet;

import wres.datamodel.scale.RescalingException;
import wres.datamodel.scale.TimeScaleOuter;

/**
 * Used to increase the {@link TimeScaleOuter} of a {@link TimeSeries}, where "increase" means to produce event values 
 * whose {@link TimeScaleOuter#getPeriod} is larger than the {@link TimeScaleOuter#getPeriod} of the input. This is also 
 * known as temporal upscaling. Here, upscaling has the opposite meaning of upscaling in the context of video encoding. 
 * In that context, upscaling means to increase the resolution or increase amount of data. Here, it means to reduce the 
 * amount of data.
 * 
 * @param <T> the type of event value
 * @author james.brown@hydrosolved.com
 */

public interface TimeSeriesUpscaler<T>
{

    /**
     * Upscales the input {@link TimeSeries} to the desired {@link TimeScaleOuter} without any constraints on the times 
     * at which the upscaled values must end. In other words, upscaling starts from the beginning of the input time-
     * series. Also see {@link #upscale(TimeSeries, TimeScaleOuter, SortedSet)}.
     * 
     * @param timeSeries the time-series to upscale
     * @param desiredTimeScale the desired time scale
     * @return an upscaled time-series, plus any validation events
     * @throws RescalingException if the time-series could not be upscaled
     * @throws NullPointerException if any input is null
     */

    RescaledTimeSeriesPlusValidation<T> upscale( TimeSeries<T> timeSeries, TimeScaleOuter desiredTimeScale );

    /**
     * Upscales the input {@link TimeSeries} to the desired {@link TimeScaleOuter} such that each upscaled value ends at 
     * one of the prescribed times. For upscaling without constraints, see {@link #upscale(TimeSeries, TimeScaleOuter)}. 
     * If no times are provided at which the upscaled values should end, the behavior matches the unconditional case, 
     * {@link #upscale(TimeSeries, TimeScaleOuter)}.
     * 
     * @param timeSeries the time-series to upscale
     * @param desiredTimeScale the desired time scale
     * @param endsAt the time at which each upscaled value should end, sorted to allow implementations to exploit order
     * @return an upscaled time-series with (up to) as many events as times in endAt, plus and validation events
     * @throws RescalingException if the time-series could not be upscaled
     * @throws NullPointerException if any input is null
     */

    RescaledTimeSeriesPlusValidation<T> upscale( TimeSeries<T> timeSeries,
                                                 TimeScaleOuter desiredTimeScale,
                                                 SortedSet<Instant> endsAt );

}
