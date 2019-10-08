package wres.datamodel.time;

import java.time.Instant;
import java.util.Set;

import wres.datamodel.scale.RescalingException;
import wres.datamodel.scale.TimeScale;

/**
 * Used to increase the {@link TimeScale} of a {@link TimeSeries}, where "increase" means to produce event values whose 
 * {@link TimeScale#getPeriod} is larger than the {@link TimeScale#getPeriod} of the input. This is also known as
 * temporal upscaling. Here, upscaling has the opposite meaning of upscaling in the context of video encoding. In that 
 * context, upscaling means to increase the resolution or increase amount of data. Here, it means to reduce the amount 
 * of data.
 * 
 * @param <T> the type of event value
 * @author james.brown@hydrosolved.com
 */

public interface TimeSeriesUpscaler<T>
{

    /**
     * Upscales the input {@link TimeSeries} to the desired {@link TimeScale} without any constraints on the times at
     * which the upscaled values must end. In other words, upscaling starts from the beginning of the input time-series. 
     * Also see {@link #upscale(TimeSeries, TimeScale, Set)}.
     * 
     * @param timeSeries the time-series to upscale
     * @param desiredTimeScale the desired time scale
     * @return an upscaled time-series, plus any validation events
     * @throws RescalingException if the time-series could not be upscaled
     * @throws NullPointerException if any input is null
     */

    RescaledTimeSeriesPlusValidation<T> upscale( TimeSeries<T> timeSeries, TimeScale desiredTimeScale );

    /**
     * Upscales the input {@link TimeSeries} to the desired {@link TimeScale} such that each upscaled value ends at one
     * of the prescribed times. For upscaling without constraints, see {@link #upscale(TimeSeries, TimeScale)}. If no
     * times are provided at which the upscaled values should end, the behavior matches the unconditional case, 
     * {@link #upscale(TimeSeries, TimeScale)}.
     * 
     * @param timeSeries the time-series to upscale
     * @param desiredTimeScale the desired time scale
     * @param endsAt the time at which each upscaled value should end
     * @return an upscaled time-series with (up to) as many events as times in endAt, plus and validation events
     * @throws RescalingException if the time-series could not be upscaled
     * @throws NullPointerException if any input is null
     */

    RescaledTimeSeriesPlusValidation<T> upscale( TimeSeries<T> timeSeries,
                                                 TimeScale desiredTimeScale,
                                                 Set<Instant> endsAt );

}
