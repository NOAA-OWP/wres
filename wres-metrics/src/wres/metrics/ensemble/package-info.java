/**
 * <p>A package for {@link wres.metrics.Metric} that consume ensemble pairs.
 *
 * <p>Implementation note: in general, ensemble metrics require sorted members. When an evaluation includes several
 * metrics that require sorted members, there is generally an advantage of sorting the members upfront, rather than
 * sorting the members for each metric calculation in turn, as sorting is much more expensive than checking for
 * sortedness.
 * 
 * @author James Brown
 */
package wres.metrics.ensemble;