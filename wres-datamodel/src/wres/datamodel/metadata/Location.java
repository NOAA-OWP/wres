package wres.datamodel.metadata;

import java.util.Objects;

/**
 * Representation of a geodetic location that comprises a latitude and longitude relative to the WGS84 spheroid, 
 * together with other identifying attributes, such as a name and gage identifier. This implementation is immutable.
 * 
 * @author chris.tubbs
 * @author james.brown@hydrosolved.com
 */

public class Location
{

    /**
     * Longitude relative to the WGS84 spheroid, may be null.
     */

    private final Float longitude;

    /**
     * Latitude relative to the WGS84 spheroid, may be null.
     */

    private final Float latitude;

    /**
     * Vector identifier, may be null.
     */

    private final Long vectorIdentifier;

    /**
     * Location name, may be null.
     */

    private final String locationName;

    /**
     * Gage identifier, may be null.
     * 
     */
    private final String gageId;

    /**
     * Returns an instance from the inputs.
     * 
     * @param vectorIdentifier the optional vector identifier
     * @param locationName the optional location name
     * @param longitude the optional longitude
     * @param latitude the optional latitude
     * @param gageId the optional gage identifier
     * @throws NullPointerException if only one of the two coordinates is null or all inputs are null
     * @return a location instance
     */

    public static Location of( Long vectorIdentifier,
                               String locationName,
                               Float longitude,
                               Float latitude,
                               String gageId )
    {
        return new Location( vectorIdentifier, locationName, longitude, latitude, gageId );
    }

    /**
     * Optional Identifier for the location in Vector space
     *
     * @return The identifier for the location in Vector space
     */
    public Long getVectorIdentifier()
    {
        return this.vectorIdentifier;
    }

    /**
     * Optional Name for the location, such as "COTA2"
     *
     * @return The name of a location, typically in an abbreviated format
     */
    public String getLocationName()
    {
        return this.locationName;
    }

    /**
     * Optional longitudinal coordinate for a location
     *
     * @return The longitude for a location
     */
    public Float getLongitude()
    {
        return this.longitude;
    }

    /**
     * Optional latitudinal coordinate for a location
     *
     * @return The latitude for a coordinate
     */
    public Float getLatitude()
    {
        return this.latitude;
    }

    /**
     * Optional gage identifier for a location
     *
     * @return The ID of the gage at a location
     */
    public String getGageId()
    {
        return this.gageId;
    }

    /**
     * Returns true if {@link #getVectorIdentifier()} returns non-null, false otherwise
     * @return true if {@link #getVectorIdentifier()} returns non-null, false otherwise
     */
    public boolean hasVectorIdentifier()
    {
        return Objects.nonNull( this.getVectorIdentifier() );
    }

    /**
     * Returns true if {@link #getLocationName()} is non-null, false otherwise
     * @return true if {@link #getLocationName()} is non-null, false otherwise
     */
    public boolean hasLocationName()
    {
        return Objects.nonNull( this.getLocationName() ) && !this.getLocationName().trim().isEmpty();
    }

    /**
     * Returns true if both {@link #getLatitude()} and {@link #getLongitude()}
     * return non-null, false otherwise
     *
     * @return true if both {@link #getLatitude()} and {@link #getLongitude()}
     * return non-null, false otherwise
     */
    public boolean hasCoordinates()
    {
        return Objects.nonNull( this.getLongitude() ) && Objects.nonNull( this.getLatitude() );
    }

    public boolean hasGageId()
    {
        return Objects.nonNull( this.getGageId() ) && !this.getGageId().trim().isEmpty();
    }

    @Override
    public String toString()
    {
        if ( this.hasLocationName() )
        {
            return this.locationName;
        }

        if ( this.hasGageId() )
        {
            return this.gageId;
        }

        if ( this.hasVectorIdentifier() )
        {
            return this.vectorIdentifier.toString();
        }

        if ( this.hasCoordinates() )
        {
            String coordinates = "" + Math.abs( this.getLongitude() );

            if ( this.getLongitude() < 0 )
            {
                coordinates += "W";
            }
            else
            {
                coordinates += "E";
            }

            coordinates += " " + Math.abs( this.getLatitude() );

            if ( this.getLatitude() < 0 )
            {
                coordinates += "S";
            }
            else
            {
                coordinates += "N";
            }

            return coordinates;
        }

        return "Unknown";
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( obj instanceof Location )
        {
            Location other = (Location) obj;

            boolean locationsEqual = true;
            boolean vectorIDsEqual = true;
            boolean gageIDsEqual = true;
            boolean coordinatesEqual = true;

            if ( this.hasLocationName() && other.hasLocationName() )
            {
                locationsEqual = this.getLocationName().equalsIgnoreCase( other.getLocationName() );
            }
            else if ( this.hasLocationName() || other.hasLocationName() )
            {
                return false;
            }

            if ( this.hasVectorIdentifier() && other.hasVectorIdentifier() )
            {
                vectorIDsEqual = this.getVectorIdentifier().equals( other.getVectorIdentifier() );
            }
            else if ( this.hasVectorIdentifier() || other.hasVectorIdentifier() )
            {
                return false;
            }

            if ( this.hasGageId() && other.hasGageId() )
            {
                gageIDsEqual = this.getGageId().equalsIgnoreCase( other.getGageId() );
            }
            else if ( this.hasGageId() || other.hasGageId() )
            {
                return false;
            }

            if ( this.hasCoordinates() && other.hasCoordinates() )
            {
                coordinatesEqual = this.getLatitude().equals( other.getLatitude() ) &&
                                   this.getLongitude().equals( other.getLongitude() );
            }
            else if ( this.hasCoordinates() || other.hasCoordinates() )
            {
                return false;
            }

            return locationsEqual && vectorIDsEqual && gageIDsEqual && coordinatesEqual;
        }

        return false;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( this.locationName,
                             this.gageId,
                             this.vectorIdentifier,
                             this.longitude,
                             this.latitude );
    }


    /**
     * Hidden constructor.
     * 
     * @param vectorIdentifier the optional vector identifier
     * @param locationName the optional location name
     * @param longitude the optional longitude
     * @param latitude the optional latitude
     * @param gageId the optional gage identifier
     * @throws NullPointerException if only one of the two coordinates is null or all inputs are null
     */

    private Location( Long vectorIdentifier,
                      String locationName,
                      Float longitude,
                      Float latitude,
                      String gageId )
    {
        if ( Objects.nonNull( latitude ) != Objects.nonNull( longitude ) )
        {
            throw new NullPointerException( "One of the two coordinates is null. Both must be null or non-null." );
        }

        if ( Objects.isNull( vectorIdentifier ) && Objects.isNull( locationName )
             && Objects.isNull( longitude )
             && Objects.isNull( latitude )
             && Objects.isNull( gageId ) )
        {
            throw new NullPointerException( "Specify at least one non-null input to qualify the location." );
        }

        this.vectorIdentifier = vectorIdentifier;
        this.locationName = locationName;
        this.longitude = longitude;
        this.latitude = latitude;
        this.gageId = gageId;
    }

}
