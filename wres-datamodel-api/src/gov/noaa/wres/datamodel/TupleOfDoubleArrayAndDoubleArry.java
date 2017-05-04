package gov.noaa.wres.datamodel;

/**
 * Use getKey to get first element which is DoubleBrick
 * Use getDoubles to get second element which is DoubleBrick
 * @author jesse
 *
 */
public interface TupleOfDoubleArrayAndDoubleArry extends DoubleBrick
{
    /** Retrieve the first value. Second value is getDoubles */
    DoubleBrick getKey();
}
