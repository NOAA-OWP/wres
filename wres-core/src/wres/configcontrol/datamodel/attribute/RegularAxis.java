package wres.configcontrol.datamodel.attribute;

public interface RegularAxis<T> extends Axis<T>
{
    //TODO See the design.  A RegularAxis is a regularly spaced axis that can be defined by a 
    //start, step, and end.  However, I'm not sure what will be in the interface that is not already
    //in the upper level Axis interface.
}
