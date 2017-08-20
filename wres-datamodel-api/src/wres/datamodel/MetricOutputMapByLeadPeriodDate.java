package wres.datamodel;

/**
 * <p>
 * A sorted map of {@link MetricOutput} associated with a single metric. The results are stored by the range of forecast
 * lead times and the duration of the historical period into which data were pooled, together with a date that denotes
 * the start or end end of the pooling period.
 * </p>
 * Example applications of this store include:
 * <ol>
 * <li>Verification within a rolling window of recent historical dates (forecast valid times) for a single forecast lead
 * time. Here, the pooling period would be non-zero, the date would represent the end of the pooling period, and the
 * lead period would include a single forecast lead time.</li>
 * <li>Descriptive statistics for the portion of a single forecast between a first and last forecast lead time. Here,
 * the pooling period would be zero, the date would represent the forecast issue time, and the lead period would include
 * the first and last lead times.</li>
 * </ol>
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public interface MetricOutputMapByLeadPeriodDate
//<T extends MetricOutput<?>>
//extends MetricOutputMapWithTriKey<LeadPeriod, Integer, ZonedDateTime, T>
{

//    /**
//     * Slice by the width of the period into which data is pooled.
//     * 
//     * @param period the forecast lead time
//     * @return the submap
//     */
//
//    default MetricOutputMapByLeadPeriodDate<T> sliceByPeriod(final Integer period)
//    {
//        return (MetricOutputMapByLeadPeriodDate<T>)sliceByFirst(period);
//    }
//
//    /**
//     * Slice by date.
//     * 
//     * @param date the date
//     * @return the submap
//     */
//
//    default MetricOutputMapByLeadPeriodDate<T> sliceByDate(final ZonedDateTime date)
//    {
//        return (MetricOutputMapByLeadPeriodDate<T>)sliceBySecond(date);
//    }
//
//    /**
//     * Return the keys corresponding to the pooling window.
//     * 
//     * @return a view of the periods
//     */
//
//    default Set<Integer> keySetByPeriod()
//    {
//        return keySetByFirstKey();
//    }
//
//    /**
//     * Return the date keys.
//     * 
//     * @return a view of the date keys
//     */
//
//    default Set<ZonedDateTime> keySetByDate()
//    {
//        return keySetBySecondKey();
//    }
//
//    /**
//     * Returns the {@link MetricOutputMetadata} associated with all {@link MetricOutput} in the store. This may contain
//     * more (optional) information than the (required) metadata associated with the individual outputs. However, all
//     * required elements must match, in keeping with {@link MetricOutputMetadata#minimumEquals(MetricOutputMetadata)}.
//     * 
//     * @return the metadata
//     */
//
//    MetricOutputMetadata getMetadata();

}
