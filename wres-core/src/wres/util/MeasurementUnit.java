package wres.util;

public enum MeasurementUnit {
	UNKNOWN,
	ONE__PER__METER,
	ABSORBANCE__PER__CENTIMETER,
	BAR,
	BEQUEREL__PER__KILOGRAM,
	BEQUEREL__PER__LITRE,
	CENTIMETER,
	DECIMETER,
	DEGREE__CELCIUS_,
	DEGREE__FARHRENHEIT_,
	DEGREE__ANGLE_,
	DISINTEGRATIONS_PER_MINUTE__PER__CUBIC_CENTIMETER_OF_KRYPTON,
	FEET,
	FEET_CUBED__PER__SECOND,
	FORMAZINE_TURPIDITY_UNITS,
	GRAM,
	GRAM__PER__KILOGRAM,
	GRAM__PER__LITRE,
	HA,
	HAZEN,
	HOUR,
	INCH,
	KILOGRAM,
	KILOGRAM__PER__DAY,
	KILOGRAM__PER__HA,
	KILOGRAM__PER__METER_SQUARED,
	KILOGRAM__PER__M3_P,
	KILOMETER,
	KILOMETER_SQUARED,
	KILOMETER_CUBED,
	LITRE,
	LITRE__PER__SECOND,
	MEGAWATT,
	METER,
	METER_CUBED__PER__ANNUM,
	METER_CUBED__PER__DAY,
	METER_CUBED__PER__HOUR,
	METER_CUBED__PER__SECOND,
	METER_SQUARED,
	METER_SQUARED__PER__DAY,
	METER__PER__DAY,
	METER__PER__SECOND,
	MICROSIEMENS__PER__CENTIMETER,
	MICROGRAM__PER__KILOGRAM,
	MICROGRAM__PER__LITRE,
	MICROGRAM__PER__METER_CUBED,
	MILLIMETER,
	MILLIMETER__PER__SECOND,
	MILLIMETER__PER__HOUR,
	MILLIMETER__PER__DAY,
	MILLIVOLT,
	MINUTE,
	NANOGRAM__PER__KILOGRAM,
	NANOGRAM__PER__LITRE,
	NEPHELOMETRIC_TURBIDITY_UNITS,
	NONE,
	NUMBER,
	NUMBER__PER__HA,
	NUMBER__PER__KM2,
	NUMBER__PER__LITRE,
	PERCENT,
	PERCENT_SATURATIONS,
	PH_UNIT,
	PICOGRAM__PER__KILOGRAM,
	PLATINUM_COBALT_UNITS,
	SECOND,
	TONS,
	TRITIUM_UNITS,
	WATT__PER__METER_SQUARED;
	
	@Override
	public String toString()
	{
		return super.toString()
				.replaceAll("__PER__", "\\")
				.replaceAll("__", " (")
				.replaceAll("_$", ")");
	}
	
	public String get_acronym()
	{
		String acronym = "";
		
		switch (this)
		{
		case TONS:
			acronym = "ton";
			break;
		case ABSORBANCE__PER__CENTIMETER:
			acronym = "apc";
			break;
		case BAR:
			acronym = "bar";
			break;
		case BEQUEREL__PER__KILOGRAM:
			acronym = "bqpkg";
			break;
		case BEQUEREL__PER__LITRE:
			acronym = "bqpl";
			break;
		case CENTIMETER:
			acronym = "cm";
			break;
		case DECIMETER:
			acronym = "dm";
			break;
		case DEGREE__ANGLE_:
			acronym = "ang";
			break;
		case DEGREE__CELCIUS_:
			acronym = "c";
			break;
		case DEGREE__FARHRENHEIT_:
			acronym = "f";
			break;
		case FEET:
			acronym = "ft";
			break;
		case FEET_CUBED__PER__SECOND:
			acronym = "CFS";
			break;
		case FORMAZINE_TURPIDITY_UNITS:
			acronym = "ftu";
			break;
		case GRAM:
			acronym = "g";
			break;
		case GRAM__PER__KILOGRAM:
			acronym = "gpkg";
			break;
		case GRAM__PER__LITRE:
			acronym = "gpl";
			break;
		case HA:
			acronym = "ha";
			break;
		case HAZEN:
			acronym = "apha";
			break;
		case HOUR:
			acronym = "hr";
			break;
		case INCH:
			acronym = "in";
			break;
		case KILOGRAM:
			acronym = "kg";
			break;
		case KILOGRAM__PER__DAY:
			acronym = "kgpd";
			break;
		case KILOGRAM__PER__HA:
			acronym = "kgpha";
			break;
		case KILOGRAM__PER__M3_P:
			acronym = "kgpcbm";
			break;
		case KILOGRAM__PER__METER_SQUARED:
			acronym = "kgpsqm";
			break;
		case KILOMETER:
			acronym = "km";
			break;
		case KILOMETER_CUBED:
			acronym = "cbkm";
			break;
		case KILOMETER_SQUARED:
			acronym = "sqkm";
			break;
		case LITRE:
			acronym = "l";
			break;
		case LITRE__PER__SECOND:
			acronym = "lps";
			break;
		case MEGAWATT:
			acronym = "mw";
			break;
		case METER:
			acronym = "m";
			break;
		case METER_CUBED__PER__ANNUM:
			acronym = "cbmpa";
			break;
		case METER_CUBED__PER__DAY:
			acronym = "cbmpd";
			break;
		case METER_CUBED__PER__HOUR:
			acronym = "cbmph";
			break;
		case METER_CUBED__PER__SECOND:
			acronym = "cbmps";
			break;
		case METER_SQUARED:
			acronym = "sqm";
			break;
		case METER_SQUARED__PER__DAY:
			acronym = "sqmpd";
			break;
		case METER__PER__DAY:
			acronym = "mpd";
			break;
		case METER__PER__SECOND:
			acronym = "mps";
			break;
		case MICROGRAM__PER__KILOGRAM:
			acronym = "mcgpkg";
			break;
		case MICROGRAM__PER__LITRE:
			acronym = "mcgpl";
			break;
		case MICROGRAM__PER__METER_CUBED:
			acronym = "mcgpcbm";
			break;
		case MICROSIEMENS__PER__CENTIMETER:
			acronym = "mspcm";
			break;
		case MILLIMETER:
			acronym = "mm";
			break;
		case MILLIMETER__PER__DAY:
			acronym = "mmpd";
			break;
		case MILLIMETER__PER__HOUR:
			acronym = "mmph";
			break;
		case MILLIMETER__PER__SECOND:
			acronym = "mmps";
			break;
		case MILLIVOLT:
			acronym = "mv";
			break;
		case MINUTE:
			acronym = "m";
			break;
		case NANOGRAM__PER__KILOGRAM:
			acronym = "ngpkg";
			break;
		case NANOGRAM__PER__LITRE:
			acronym = "ngpl";
			break;
		case NEPHELOMETRIC_TURBIDITY_UNITS:
			acronym = "ntu";
			break;
		case NUMBER:
			acronym = "n";
			break;
		case NUMBER__PER__HA:
			acronym = "npha";
			break;
		case NUMBER__PER__KM2:
			acronym = "npkm2";
			break;
		case NUMBER__PER__LITRE:
			acronym = "npl";
			break;
		case ONE__PER__METER:
			acronym = "1pm";
			break;
		case PERCENT:
			acronym = "%";
			break;
		case PERCENT_SATURATIONS:
			acronym = "persat";
			break;
		case PH_UNIT:
			acronym = "ph";
			break;
		case PICOGRAM__PER__KILOGRAM:
			acronym = "pcpkg";
			break;
		case PLATINUM_COBALT_UNITS:
			acronym = "pcu";
			break;
		case SECOND:
			acronym = "s";
			break;
		case TRITIUM_UNITS:
			acronym = "tu";
			break;
		case WATT__PER__METER_SQUARED:
			acronym = "wpsqm";
			break;
		case DISINTEGRATIONS_PER_MINUTE__PER__CUBIC_CENTIMETER_OF_KRYPTON:
		case UNKNOWN:
		case NONE:
		default:
			break;
		}
		
		return acronym;
	}

	public static MeasurementUnit value_by_name(String name)
	{
		name = name.toUpperCase()
				.replaceAll("\\", "__PER__")
				.replaceAll(" (", "__")
				.replaceAll(")", "_");
		return MeasurementUnit.valueOf(name);
	}
	
	/**
	 * 
	 * @param acronym
	 * @return
	 */
	public static MeasurementUnit value_by_acronym(String acronym)
	{
		MeasurementUnit unit;
		acronym = acronym.toLowerCase();
		
		switch (acronym)
		{
		case "bar":
			unit = BAR;
			break;
		case "cm":
			unit = CENTIMETER;
			break;
		case "dm":
			unit = DECIMETER;
			break;
		case "c":
			unit = DEGREE__CELCIUS_;
			break;
		case "f":
			unit = DEGREE__FARHRENHEIT_;
			break;
		case "ft":
			unit = FEET;
			break;
		case "g":
			unit = GRAM;
			break;
		case "apha":
			unit = HAZEN;
			break;
		case "hr":
			unit = HOUR;
			break;
		case "in":
			unit = INCH;
			break;
		case "kg":
			unit = KILOGRAM;
			break;
		case "km":
			unit = KILOMETER;
			break;
		case "sqkm":
			unit = KILOMETER_SQUARED;
			break;
		case "ckm":
			unit = KILOMETER_CUBED;
			break;
		case "l":
			unit = LITRE;
			break;
		case "lps":
			unit = LITRE__PER__SECOND;
			break;
		case "mw":
			unit = MEGAWATT;
			break;
		case "m":
			unit = METER;
			break;
		case "mm":
			unit = MILLIMETER;
			break;
		case "mv":
			unit = MILLIVOLT;
			break;
		case "%":
			unit = PERCENT;
			break;
		case "ph":
			unit = PH_UNIT;
			break;
		case "s":
			unit = SECOND;
			break;
		default:
			unit = UNKNOWN;
		}
		
		return unit;
	}
}
