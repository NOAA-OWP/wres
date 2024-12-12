/**
 * This is where an evaluation pipeline is created. An evaluation pipeline involves reading and ingesting time-series,
 * retrieving time-series and placing them into pools (performing rescaling and pairing, as needed), calculating 
 * statistics by applying metric functions to pools and, finally, writing statistics to formats. The application of a
 * metric to a pool produces a statistic.
 */

package wres.pipeline;