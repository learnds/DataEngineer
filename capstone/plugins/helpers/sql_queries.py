class SqlQueries:
    visitors_fact_insert = ("""
        Insert into public.i94visitors_fact
        SELECT
                md5(to_char(a.arrivaldate,"MMDDYYY"||a.cicid) visitorid,
                to_date('01/01/1960','MM/DD/YYY') +  interval '1' day * arrdate as arrivaldate, 
                b.airportid,
                d.stateid,
                c.countryid,
                substring(i94yr,1,instr(i94yr,'.',-1))||'-'||substring(i94mon,1,instr(i94mon,'.',-1)),
                i94mode, 
                i94visa, 
                to_date('01/01/1960','MM/DD/YYY') +  interval '1' day * depdate, 
                i9b4ir,
                i94visa,
                visapost,
                gender,
                airline,
                fltno,
                visatype
            FROM public.stage_i94visitors a,
            airports_dim b,
            countries_dim c,
            states_dim d
            where a.i94port = b.airportid
            and a.i94cit = c.countryid
            and a.i94addr = d.stateid
    """)

    dates_dim_insert = ("""
        INSERT into public.dates_dim
        SELECT arrivaldate,
        date_part("dow",arrivaldate),
        date_part("week",arrivaldate),
        date_part("month",arrivaldate),
        date_part("year",arrivaldate)
        FROM ( SELECT distinct to_date('01/01/1960','MM/DD/YYY') +  interval '1' day * arrdate as arrivaldate,
                FROM public.i94visitors_fact
                ) a
    """)

