class SqlQueries:
    visitors_fact_insert = ("""
      Insert into public.i94visitors_fact
        SELECT
                md5(cast(cicid as varchar)||cast(arrivaldate as varchar)) visitorid,
                dateadd(day, cast(arrivaldate as integer),'1960-01-01')  as arrivaldate,  
                nvl(b.airportid,'-1'),
                nvl(d.stateid,'-1'),
                nvl(c.countryid,'-1'),
                cast(cast(i94yr as varchar)||'-'||cast(i94mon as varchar)||'-01' as date) i94date,
                i94mode, 
                i94visa, 
               dateadd(day, cast(departuredate as integer),'1960-01-01'), 
                i94bir,
                visapost,
                gender,
                airline,
                fltno,
                visatype
            FROM public.stage_i94visitors a,
            airports_dim b,
            countries_dim c,
            states_dim d
            where a.i94port = b.airportid(+)
           and cast(a.i94cit as varchar) = c.countryid(+)
            and cast(a.i94addr as varchar) = d.stateid(+)
    """)

    dates_dim_insert = ("""
        INSERT into public.dates_dim
        SELECT arrivaldate,
        date_part("dow",arrivaldate),
        date_part("week",arrivaldate),
       date_part("mon",arrivaldate),
        date_part("year",arrivaldate)
        FROM ( SELECT distinct dateadd(day, cast(arrivaldate as integer),'1960-01-01') as arrivaldate
                FROM public.stage_i94visitors 
                ) a
        order by arrivaldate
    """)

