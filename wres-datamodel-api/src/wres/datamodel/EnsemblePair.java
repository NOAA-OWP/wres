package wres.datamodel;

/**
 * A usefully-named high level type but also able to be used by layers
 * that prefer to not know about what's inside (these layers can use
 * the lower-level interface of TupleOfDoubleAndDoubleArray).
 * 
 * For example, at the highest level, we care about EnsemblePair,
 * we use getForecast and getObservation (which are just pass-through
 * functions to the lower level functions). But an EnsemblePair _is_
 * a TupleOfDoubleAndDoubleArray so when a lower level is passed an
 * instance, it can call its favorite lower level getItemOne() and
 * getItemTwo()
 * 
 * May or may not be useful to have the middle layers? What do you think?
 * 
 * @author jesse
 *
 */
public interface EnsemblePair extends TupleOfDoubleAndDoubleArray
{
    public double getObservation();
    public double[] getForecast();
}
