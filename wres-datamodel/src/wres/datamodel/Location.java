package wres.datamodel;

import java.util.Objects;

public interface Location
{
    /**
     * Optional Identifier for the location in Vector space
     *
     * @return The identifier for the location in Vector space
     */
    Long getVectorIdentifier();

    /**
     * Optional Name for the location, such as "COTA2"
     *
     * @return The name of a location, typically in an abbreviated format
     */
    String getLocationName();

    /**
     * Optional longitudinal coordinate for a location
     *
     * @return The longitude for a location
     */
    Float getLongitude();

    /**
     * Optional latitudinal coordinate for a location
     *
     * @return The latitude for a coordinate
     */
    Float getLatitude();

    /**
     * Optional gage identifier for a location
     *
     * @return The ID of the gage at a location
     */
    String getGageId();

    /**
     * Returns true if {@link #getVectorIdentifier()} returns non-null, false otherwise
     * @return true if {@link #getVectorIdentifier()} returns non-null, false otherwise
     */
    default boolean hasVectorIdentifier()
    {
        return Objects.nonNull(this.getVectorIdentifier());
    }

    /**
     * Returns true if {@link #getLocationName()} is non-null, false otherwise
     * @return true if {@link #getLocationName()} is non-null, false otherwise
     */
    default boolean hasLocationName()
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
    default boolean hasCoordinates()
    {
        return Objects.nonNull( this.getLongitude() ) && Objects.nonNull( this.getLatitude() );
    }

    default boolean hasGageId()
    {
        return Objects.nonNull( this.getGageId() ) && !this.getGageId().trim().isEmpty();
    }
}
