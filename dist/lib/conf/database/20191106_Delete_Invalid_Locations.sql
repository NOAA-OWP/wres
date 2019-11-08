-- RDAC2: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'RDAC2'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW039: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW039'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW040: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW040'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW053: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW053'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW052: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW052'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW034: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW034'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW045: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW045'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW165: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW165'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW090: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW090'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW135: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW135'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW083: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW083'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW261: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW261'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW186: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW186'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW188: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW188'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW282: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW282'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW306: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW306'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW313: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW313'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW428: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW428'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW344: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW344'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW399: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW399'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW407: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW407'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW341: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW341'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW340: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW340'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW343: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW343'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW394: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW394'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW345: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW345'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW348: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW348'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW362: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW362'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW363: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW363'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW350: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW350'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW355: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW355'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW463: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW463'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW458: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW458'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW474: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW474'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW436: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW436'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW432: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW432'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW488: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW488'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW489: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW489'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW495: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW495'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW529: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW529'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW456: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW456'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW455: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW455'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW454: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW454'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW487: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW487'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW516: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW516'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW446: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW446'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW519: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW519'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW521: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW521'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW522: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW522'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW499: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW499'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- TORQ8: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'TORQ8'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW371: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW371'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW544: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW544'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW540: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW540'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW548: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW548'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW533: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW533'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- LRBM8: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'LRBM8'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW041: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW041'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW137: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW137'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW504: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW504'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW047: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW047'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW374: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW374'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW373: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW373'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW431: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW431'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW416: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW416'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW105: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW105'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW258: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW258'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW257: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW257'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW134: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW134'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW304: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW304'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW175: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW175'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW413: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW413'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW408: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW408'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW410: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW410'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW490: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW490'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW460: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW460'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW176: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW176'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW483: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW483'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW078: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW078'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW392: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW392'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW085: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW085'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW024: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW024'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW022: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW022'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW098: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW098'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW023: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW023'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW099: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW099'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW326: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW326'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW500: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW500'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW501: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW501'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW502: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW502'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW042: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW042'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW043: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW043'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW325: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW325'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW451: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW451'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW472: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW472'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW097: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW097'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW096: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW096'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW021: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW021'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW434: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW434'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW106: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW106'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW390: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW390'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW389: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW389'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW530: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW530'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- JEFL1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'JEFL1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW056: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW056'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW333: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW333'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW334: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW334'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW300: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW300'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW493: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW493'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW281: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW281'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW274: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW274'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW280: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW280'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW264: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW264'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW277: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW277'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW384: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW384'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- CWXW1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'CWXW1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW260: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW260'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- PTET2: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'PTET2'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW441: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW441'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW442: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW442'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW279: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW279'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW026: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW026'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW167: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW167'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW001: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW001'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW430: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW430'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW020: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW020'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW549: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW549'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW386: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW386'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW485: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW485'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW118: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW118'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW119: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW119'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW121: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW121'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW217: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW217'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW473: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW473'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW117: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW117'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW107: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW107'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW108: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW108'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW109: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW109'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW550: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW550'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW177: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW177'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW076: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW076'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW081: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW081'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW146: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW146'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW148: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW148'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW025: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW025'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW027: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW027'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW314: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW314'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW185: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW185'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW170: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW170'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW171: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW171'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW062: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW062'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW412: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW412'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW156: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW156'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW467: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW467'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW468: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW468'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- ANDI1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'ANDI1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW205: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW205'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW044: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW044'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW360: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW360'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW367: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW367'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW069: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW069'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW423: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW423'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW425: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW425'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW424: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW424'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW245: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW245'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW421: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW421'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NW422: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NW422'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- TRAM5: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'TRAM5'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- INDN6: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'INDN6'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- ALKT2: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'ALKT2'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- HRLN6: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'HRLN6'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- AKLN6: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'AKLN6'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- STIP1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'STIP1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- AYLP1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'AYLP1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- PMTP1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'PMTP1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- CREA1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'CREA1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- WPKA1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'WPKA1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- MHDA1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'MHDA1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- GARN8: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'GARN8'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- GLUN8: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'GLUN8'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- CLLV2: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'CLLV2'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- FTEQ6: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'FTEQ6'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- BURV1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'BURV1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- ROUN6: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'ROUN6'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- LMIS1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'LMIS1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- JACW2: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'JACW2'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- BRLO1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'BRLO1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- KIRO1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'KIRO1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- LEXO1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'LEXO1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- HTRK1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'HTRK1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- WDHN2: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'WDHN2'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NANN7: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NANN7'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- GLNK1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'GLNK1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- STCK1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'STCK1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- CHLI3: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'CHLI3'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- CAGI3: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'CAGI3'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- POEF1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'POEF1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- MCFM6: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'MCFM6'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- ARHT2: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'ARHT2'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- ARBO2: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'ARBO2'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- KNGN1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'KNGN1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- HUNI3: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'HUNI3'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- SLAI3: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'SLAI3'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- MSSI3: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'MSSI3'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- TLDN1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'TLDN1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- WLON1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'WLON1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NWKN4: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NWKN4'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- BOON4: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'BOON4'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- BRVO3: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'BRVO3'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- BADN7: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'BADN7'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- CBKS2: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'CBKS2'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- KBYT2: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'KBYT2'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- LCRT2: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'LCRT2'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- SKCT2: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'SKCT2'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- GCDW1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'GCDW1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- GDDP1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'GDDP1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- CNGK1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'CNGK1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- JRLK1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'JRLK1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- MAYO2: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'MAYO2'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- CPLO2: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'CPLO2'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- KERO2: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'KERO2'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- WFLO2: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'WFLO2'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- EDRK1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'EDRK1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- TRLK1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'TRLK1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- ACRO1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'ACRO1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- LEEM5: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'LEEM5'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- MLLM5: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'MLLM5'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- SHTQ8: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'SHTQ8'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- DMSN8: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'DMSN8'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- LNRC1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'LNRC1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- ADDT2: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'ADDT2'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- AGTG1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'AGTG1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- ALAT2: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'ALAT2'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- BCKO1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'BCKO1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- BHDO1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'BHDO1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- CGRN5: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'CGRN5'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- CJBO1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'CJBO1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- CVLG1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'CVLG1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- CVQC1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'CVQC1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- DIXK2: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'DIXK2'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- EGLN5: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'EGLN5'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- FLWT2: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'FLWT2'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- FRHT2: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'FRHT2'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- GRET2: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'GRET2'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- HWDS1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'HWDS1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- JPLT2: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'JPLT2'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- KNZP1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'KNZP1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- LART2: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'LART2'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- MONI3: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'MONI3'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- PNTT2: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'PNTT2'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- PRLI3: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'PRLI3'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- SRLN5: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'SRLN5'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- TIOP1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'TIOP1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- UTEN5: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'UTEN5'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- WPPM7: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'WPPM7'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- AXTV2: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'AXTV2'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- PINS1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'PINS1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- PNTK2: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'PNTK2'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- LOSO3: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'LOSO3'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- SFRW1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'SFRW1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- YARW1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'YARW1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- BCVC1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'BCVC1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- WLHN2: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'WLHN2'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- RICV2: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'RICV2'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- FTMV2: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'FTMV2'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- JSFV2: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'JSFV2'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- SRDM2: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'SRDM2'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- MEAP1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'MEAP1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- MISO1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'MISO1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- WSPC2: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'WSPC2'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- RVST1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'RVST1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- ALLM6: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'ALLM6'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- WHNP1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'WHNP1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- PRYP1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'PRYP1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- SHPP1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'SHPP1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- CRCP1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'CRCP1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- MHGP1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'MHGP1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- LYLP1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'LYLP1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- BSZM7: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'BSZM7'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- LOPO3: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'LOPO3'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- BIGW4: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'BIGW4'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- PHBQ7: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'PHBQ7'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- BUSP1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'BUSP1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- COWP1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'COWP1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- CRWP1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'CRWP1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- RTDP1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'RTDP1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- SYDP1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'SYDP1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- STVP1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'STVP1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- TGAP1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'TGAP1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- YGBP1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'YGBP1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- VARN6: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'VARN6'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- MRGL1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'MRGL1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- STJQ7: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'STJQ7'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- NUTQ7: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'NUTQ7'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- BURO3: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'BURO3'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
-- HOOO1: Only Delete if there's no active data
DELETE FROM wres.Feature F 
WHERE lid = 'HOOO1'
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.TimeSeries TS
            ON TS.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM wres.VariableFeature VF
        INNER JOIN wres.Observation O
            ON O.variablefeature_id = VF.variablefeature_id
        WHERE VF.feature_id = F.feature_id   
    );
    
