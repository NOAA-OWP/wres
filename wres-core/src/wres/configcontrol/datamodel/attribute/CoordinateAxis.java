package wres.configcontrol.datamodel.attribute;

public interface CoordinateAxis extends Axis<Double>
{
    //TODO CoordinateAxis is an interface that must be implemented by axes for lat/lon and time (valid time, lead time).
    //I'm not sure what additional methods would be implemented in this as opposed to just Axis.
}
