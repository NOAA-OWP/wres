package wres.config.components;

import com.fasterxml.jackson.annotation.JsonProperty;

import wres.config.DeclarationUtilities;

/**
 * An enumeration of statistics formats.
 * @author James Brown
 */
public enum Format
{
    /** Portable Network Graphics (PNG). */
    @JsonProperty( "png" ) PNG,
    /** Scalable Vector Graphics (SVG). */
    @JsonProperty( "svg" ) SVG,
    /** Comma Separated Values (CSV). */
    @JsonProperty( "csv" ) CSV,
    /** CSV Version 2.0. */
    @JsonProperty( "csv2" ) CSV2,
    /** Pairs in CSV format. */
    @JsonProperty( "pairs" ) PAIRS,
    /** Network Common Data Form (NetCDF). */
    @JsonProperty( "netcdf" ) NETCDF,
    /** NETCDF Version 2.0. */
    @JsonProperty( "netcdf2" ) NETCDF2,
    /** Protocol Buffers. */
    @JsonProperty( "protobuf" ) PROTOBUF,
    /** A bag of graphics formats. This is not part of the declaration language, but is used internally. */
    GRAPHIC;

    /**
     * @return whether this format is a graphics format
     */

    public boolean isGraphicsFormat()
    {
        return this == PNG || this == SVG || this == GRAPHIC;
    }

    @Override
    public String toString()
    {
        return DeclarationUtilities.fromEnumName( this.name() );
    }
}
