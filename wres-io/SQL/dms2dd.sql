-- Function: public.dms2dd(character varying)

-- DROP FUNCTION public.dms2dd(character varying);

CREATE OR REPLACE FUNCTION public.dms2dd(strdegminsec character varying)
  RETURNS numeric AS
$BODY$
    DECLARE
       i               numeric;
       intDmsLen       numeric;          -- Length of original string
       strCompassPoint Char(1);
       strNorm         varchar(16) = ''; -- Will contain normalized string
       strDegMinSecB   varchar(100);
       blnGotSeparator integer;          -- Keeps track of separator sequences
       arrDegMinSec    varchar[];        -- TYPE stringarray is table of varchar(2048) ;
       dDeg            numeric := 0;
       dMin            numeric := 0;
       dSec            numeric := 0;
       strChr          Char(1);
    BEGIN
       -- Remove leading and trailing spaces
       strDegMinSecB := REPLACE(strDegMinSec,' ','');
       -- assume no leading and trailing spaces?
       intDmsLen := Length(strDegMinSecB);

       blnGotSeparator := 0; -- Not in separator sequence right now

       -- Loop over string, replacing anything that is not a digit or a
       -- decimal separator with
       -- a single blank
       FOR i in 1..intDmsLen LOOP
          -- Get current character
          strChr := SubStr(strDegMinSecB, i, 1);
          -- either add character to normalized string or replace
          -- separator sequence with single blank         
          If strpos('0123456789,.', strChr) > 0 Then
             -- add character but replace comma with point
             If (strChr <> ',') Then
                strNorm := strNorm || strChr;
             Else
                strNorm := strNorm || '.';
             End If;
             blnGotSeparator := 0;
          ElsIf strpos('neswNESW',strChr) > 0 Then -- Extract Compass Point if present
            strCompassPoint := strChr;
          Else
             -- ensure only one separator is replaced with a blank -
             -- suppress the rest
             If blnGotSeparator = 0 Then
                strNorm := strNorm || ' ';
                blnGotSeparator := 0;
             End If;
          End If;
       End Loop;

       -- Split normalized string into array of max 3 components
       arrDegMinSec := string_to_array(strNorm, ' ');

       --convert specified components to double
       i := array_upper(arrDegMinSec,1);
       If i >= 1 Then
          dDeg := CAST(arrDegMinSec[1] AS numeric);
       End If;
       If i >= 2 Then
          dMin := CAST(arrDegMinSec[2] AS numeric);
       End If;
       If i >= 3 Then
          dSec := CAST(arrDegMinSec[3] AS numeric);
       End If;

       -- convert components to value
       return (CASE WHEN UPPER(strCompassPoint) IN ('S','W') 
                    THEN -1 
                    ELSE 1 
                END 
               *
               (dDeg + dMin / 60 + dSec / 3600));
    End 
$BODY$
  LANGUAGE plpgsql IMMUTABLE
  COST 100;
