-- miniproject.dbo.avg_trip_dist definition

-- Drop table

-- DROP TABLE miniproject.dbo.avg_trip_dist;

CREATE TABLE miniproject.dbo.avg_trip_dist (
	borough varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	avg_trip_distance float NULL,
	CONSTRAINT PK__avg_trip__DEB7A9BD9E5492FA PRIMARY KEY (borough)
);


-- miniproject.dbo.common_locations definition

-- Drop table

-- DROP TABLE miniproject.dbo.common_locations;

CREATE TABLE miniproject.dbo.common_locations (
	location_type varchar(10) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	[zone] varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	trip_count bigint NULL,
	CONSTRAINT PK__common_l__08DEBE4D8F22E0B1 PRIMARY KEY (location_type,[zone])
);


-- miniproject.dbo.daily_trip_totals definition

-- Drop table

-- DROP TABLE miniproject.dbo.daily_trip_totals;

CREATE TABLE miniproject.dbo.daily_trip_totals (
	[date] date NOT NULL,
	total_trips int NULL,
	CONSTRAINT PK__daily_tr__D9DE21FC867F962B PRIMARY KEY ([date])
);


-- miniproject.dbo.date_dim definition

-- Drop table

-- DROP TABLE miniproject.dbo.date_dim;

CREATE TABLE miniproject.dbo.date_dim (
	date_id int IDENTITY(1,1) NOT NULL,
	[date] date NOT NULL,
	[year] int NOT NULL,
	[month] int NOT NULL,
	[day] int NOT NULL,
	weekday varchar(10) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	CONSTRAINT PK__date_dim__51FC4865E3C9104B PRIMARY KEY (date_id)
);


-- miniproject.dbo.fhv definition

-- Drop table

-- DROP TABLE miniproject.dbo.fhv;

CREATE TABLE miniproject.dbo.fhv (
	dispatching_base_num nvarchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	pickup_datetime datetime NULL,
	dropOff_datetime datetime NULL,
	PUlocationID float NULL,
	DOlocationID float NULL,
	SR_Flag int NULL,
	Affiliated_base_number nvarchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL
);


-- miniproject.dbo.fhvhv definition

-- Drop table

-- DROP TABLE miniproject.dbo.fhvhv;

CREATE TABLE miniproject.dbo.fhvhv (
	hvfhs_license_num nvarchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	dispatching_base_num nvarchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	originating_base_num nvarchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	request_datetime datetime NULL,
	on_scene_datetime datetime NULL,
	pickup_datetime datetime NULL,
	dropoff_datetime datetime NULL,
	PULocationID bigint NULL,
	DOLocationID bigint NULL,
	trip_miles float NULL,
	trip_time bigint NULL,
	base_passenger_fare float NULL,
	tolls float NULL,
	bcf float NULL,
	sales_tax float NULL,
	congestion_surcharge float NULL,
	airport_fee float NULL,
	tips float NULL,
	driver_pay float NULL,
	shared_request_flag nvarchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	shared_match_flag nvarchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	access_a_ride_flag nvarchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	wav_request_flag nvarchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	wav_match_flag nvarchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL
);
 CREATE NONCLUSTERED INDEX fhvhv_pickup_datetime_IDX ON dbo.fhvhv (  pickup_datetime ASC  , PULocationID ASC  , dropoff_datetime ASC  , DOLocationID ASC  )  
	 WITH (  PAD_INDEX = OFF ,FILLFACTOR = 100  ,SORT_IN_TEMPDB = OFF , IGNORE_DUP_KEY = OFF , STATISTICS_NORECOMPUTE = OFF , ONLINE = OFF , ALLOW_ROW_LOCKS = ON , ALLOW_PAGE_LOCKS = ON  )
	 ON [PRIMARY ] ;


-- miniproject.dbo.green definition

-- Drop table

-- DROP TABLE miniproject.dbo.green;

CREATE TABLE miniproject.dbo.green (
	VendorID bigint NULL,
	lpep_pickup_datetime datetime NULL,
	lpep_dropoff_datetime datetime NULL,
	store_and_fwd_flag nvarchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	RatecodeID float NULL,
	PULocationID bigint NULL,
	DOLocationID bigint NULL,
	passenger_count float NULL,
	trip_distance float NULL,
	fare_amount float NULL,
	extra float NULL,
	mta_tax float NULL,
	tip_amount float NULL,
	tolls_amount float NULL,
	ehail_fee int NULL,
	improvement_surcharge float NULL,
	total_amount float NULL,
	payment_type float NULL,
	trip_type float NULL,
	congestion_surcharge float NULL
);


-- miniproject.dbo.location_dim definition

-- Drop table

-- DROP TABLE miniproject.dbo.location_dim;

CREATE TABLE miniproject.dbo.location_dim (
	location_id int NOT NULL,
	borough varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[zone] varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	service_zone varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	CONSTRAINT PK__location__771831EA969C666B PRIMARY KEY (location_id)
);


-- miniproject.dbo.taxi_zone definition

-- Drop table

-- DROP TABLE miniproject.dbo.taxi_zone;

CREATE TABLE miniproject.dbo.taxi_zone (
	LocationID bigint NULL,
	Borough varchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Zone] varchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	service_zone varchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL
);


-- miniproject.dbo.top_zone_fare definition

-- Drop table

-- DROP TABLE miniproject.dbo.top_zone_fare;

CREATE TABLE miniproject.dbo.top_zone_fare (
	[zone] varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	total_fare float NULL,
	CONSTRAINT PK__top_zone__8C3A2766D0714796 PRIMARY KEY ([zone])
);


-- miniproject.dbo.total_tip_passenger definition

-- Drop table

-- DROP TABLE miniproject.dbo.total_tip_passenger;

CREATE TABLE miniproject.dbo.total_tip_passenger (
	passenger_count int NOT NULL,
	total_tip float NULL,
	CONSTRAINT PK__total_ti__39DA72FF45961ECC PRIMARY KEY (passenger_count)
);


-- miniproject.dbo.yellow definition

-- Drop table

-- DROP TABLE miniproject.dbo.yellow;

CREATE TABLE miniproject.dbo.yellow (
	VendorID bigint NULL,
	tpep_pickup_datetime datetime NULL,
	tpep_dropoff_datetime datetime NULL,
	passenger_count float NULL,
	trip_distance float NULL,
	RatecodeID float NULL,
	store_and_fwd_flag nvarchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	PULocationID bigint NULL,
	DOLocationID bigint NULL,
	payment_type bigint NULL,
	fare_amount float NULL,
	extra float NULL,
	mta_tax float NULL,
	tip_amount float NULL,
	tolls_amount float NULL,
	improvement_surcharge float NULL,
	total_amount float NULL,
	congestion_surcharge float NULL,
	airport_fee float NULL
);


-- miniproject.dbo.trips_fact definition

-- Drop table

-- DROP TABLE miniproject.dbo.trips_fact;

CREATE TABLE miniproject.dbo.trips_fact (
	trip_id uniqueidentifier DEFAULT newid() NOT NULL,
	pickup_datetime datetime NOT NULL,
	dropoff_datetime datetime NOT NULL,
	passenger_count int NULL,
	trip_distance float NULL,
	pickup_location_id int NULL,
	dropoff_location_id int NULL,
	fare_amount float NULL,
	tip_amount float NULL,
	total_amount float NULL,
	CONSTRAINT PK__trips_fa__302A5D9EDFE4B97F PRIMARY KEY (trip_id),
	CONSTRAINT trips_fact_unique UNIQUE (pickup_datetime,pickup_location_id,dropoff_datetime,dropoff_location_id),
	CONSTRAINT FK__trips_fac__dropo__46E78A0C FOREIGN KEY (dropoff_location_id) REFERENCES miniproject.dbo.location_dim(location_id),
	CONSTRAINT FK__trips_fac__picku__45F365D3 FOREIGN KEY (pickup_location_id) REFERENCES miniproject.dbo.location_dim(location_id)
);