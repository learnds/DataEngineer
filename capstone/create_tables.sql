CREATE TABLE public.countries_dim (
	countryid varchar(10) NOT NULL,
	countryname varchar(256),
    CONSTRAINT countries_pkey PRIMARY KEY (countryid)
)


CREATE TABLE public.states_dim (
	stateid varchar(10) NOT NULL,
	statename varchar(256),
	totalpopulation numeric(12,0),
    foreignborn numeric(12,0),
	CONSTRAINT states_pkey PRIMARY KEY (stateid)
)


CREATE TABLE public."dates_dim" (
	"date" NOT NULL,
	"day" int4,
	week int4,
	"month" varchar(256),
	"year" int4,
	weekday varchar(256),
	CONSTRAINT time_pkey PRIMARY KEY ("date")
)

CREATE TABLE public.airports_dim (
	airportid varchar(10) null,
    identifier varchar(10)  null,
    type varchar(255) not null,
    name varchar(2000)  null,
    continent varchar(10)  null,
	isocountry varchar(256) null,
    isoregion varchar(10) null,
    municipality varchar(255) null,
    gpscode varchar(10) null,
    localcode varchar(10) null,
    coordinates varchar(255) null,
    CONSTRAINT airport_pkey PRIMARY KEY (airportid)
)

CREATE TABLE public.stage_i94visitors (
    cicid double precision null,
	i94yr double precision null,
    i94mon double precision null,
    i94cit double precision  null,
    i94port varchar(10)  null,
    arrivaldate double precision null,
    i94mode double precision null,
	i94addr varchar(256) null,
    departuredate double precision null,
    i94bir double precision null,
    i94visa double precision null,
    visapost varchar(10) null,
    gender varchar(10) null,
    airline varchar(10) null,
    fltno varchar(10) null,
    visatype varchar(10) null
)

CREATE TABLE public.i94visitors_fact (
	visitorid varchar(255) not null,
    arrivaldate date date not null,
    airportid varchar(10) not null,
    stateid varchar(10) not null,
    visitorcountryid varchar(10) not null,
    i94yrmon varchar(8),
    arrivalmode varchar(10) not null,
    reasonforvisit varchar(10)  null,
    departuredate date  null,
    visitorage float  null,
    visatype varchar(10) null,
	visaissuedloc varchar(10) null,
	gender varchar(10) null,
	airline varchar(10),
    fltno varchar(10),
	CONSTRAINT visitorsi94_pkey PRIMARY KEY (visitorid)
)




