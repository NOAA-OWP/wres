package wres.reading.nwis.iv.response.variable;

import java.io.Serial;
import java.io.Serializable;

import lombok.Getter;
import lombok.Setter;

/**
 * An option.
 */
@Setter
@Getter
public class Option implements Serializable
{
    @Serial
    private static final long serialVersionUID = 142625806918589450L;

    /** Name. */
    private String name;

    /** Option code. */
    private String optionCode;
}
