package wres.config;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * Small value class to hold variable names.
 * @param leftVariableName the left variable name
 * @param rightVariableName the right variable name
 * @param baselineVariableName the baseline variable name
 * @param covariateVariableNames the covariate variable names
 * @author James Brown
 */

public record VariableNames( String leftVariableName,
                             String rightVariableName,
                             String baselineVariableName,
                             Set<String> covariateVariableNames )
{
    /**
     * @param leftVariableName the left variable name
     * @param rightVariableName the right variable name
     * @param baselineVariableName the baseline variable name
     * @param covariateVariableNames the covariate variable names
     */
    public VariableNames( String leftVariableName,
                          String rightVariableName,
                          String baselineVariableName,
                          Set<String> covariateVariableNames )
    {
        this.leftVariableName = leftVariableName;
        this.rightVariableName = rightVariableName;
        this.baselineVariableName = baselineVariableName;

        if ( Objects.nonNull( covariateVariableNames ) )
        {
            this.covariateVariableNames = Collections.unmodifiableSet( covariateVariableNames );
        }
        else
        {
            this.covariateVariableNames = Set.of();
        }
    }
}
