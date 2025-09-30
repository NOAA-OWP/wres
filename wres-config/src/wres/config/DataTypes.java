package wres.config;

import wres.config.components.DataType;

/**
 * Small value class to hold data types.
 * @param leftType the left data type, inferred from ingest
 * @param rightType the left data type, inferred from ingest
 * @param baselineType the left data type, inferred from ingest
 * @param covariatesType the left data type, inferred from ingest
 * @author James Brown
 */

public record DataTypes( DataType leftType,
                         DataType rightType,
                         DataType baselineType,
                         DataType covariatesType ) {}
