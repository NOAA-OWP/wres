package wres.reading.nwis.iv.response.variable;

import java.io.Serial;
import java.io.Serializable;

import lombok.Getter;
import lombok.Setter;

/**
 * Variable options.
 */
@Setter
@Getter
public class VariableOptions implements Serializable
{
    @Serial
    private static final long serialVersionUID = -3592017658139874856L;

    /** Option. */
    private Option[] option;
}
