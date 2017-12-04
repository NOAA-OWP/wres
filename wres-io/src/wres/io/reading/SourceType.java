package wres.io.reading;

import wres.util.Internal;

/**
 * @author ctubbs
 *
 */
// TODO: This needs to be changed to use the formats from the configuration
@Deprecated
@Internal(exclusivePackage = "wres.io")
public enum SourceType {
	ASCII,
	DATACARD,
	NETCDF,
	PI_XML,
	USGS,
	WATERML,
	ARCHIVE,
	UNDEFINED
}
