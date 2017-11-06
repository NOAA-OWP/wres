UPDATE nwm_location
	SET nws_lat = round(
		dms2dd(
			trim(
				regexp_replace(
					regexp_replace(
						regexp_replace(trim(nws_lat), '^\d*(?=\s)', substring(trim(nws_lat) from '^\d*(?=\s)') || ''), 
						'\s+\d*(?=\s)', 
						substring(trim(nws_lat) from '\s+\d*(?=\s)') || ''''), 
					'  ', 
					' ')
			)
		), 
	5),
	nws_lon = round(
		dms2dd(
			trim(
				regexp_replace(
					regexp_replace(
						regexp_replace(trim(nws_lon), '^\d*(?=\s)', substring(trim(nws_lon) from '^\d*(?=\s)') || ''), 
						'\s+\d*(?=\s)', 
						substring(trim(nws_lon) from '\s+\d*(?=\s)') || ''''), 
					'  ', 
					' ')
			) || 'W'
		), 
	5)
WHERE trim(nws_lat) != '' AND trim(nws_lon) != '' AND nws_lat IS NOT null AND nws_lon IS NOT null;