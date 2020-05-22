@0xda12c5ce18c82233;

using Java = import "capnpJava/java.capnp";
$Java.package("wres.statistics.generated.capnp");
$Java.outerClassname("GeometryOuter");

struct Geometry
{
    # Elementary representation of a geometry, beginning with a named point 
    # location.

    latitude @0 :Float64;
    # WGS84 latitude.

    longitude @1 :Float64;
    # WGS84 longitude.

    name @2 :Text; 
    # User-friendly name (e.g. DRRC2)
}