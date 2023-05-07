CREATE SCHEMA IF NOT EXISTS target;

CREATE TABLE IF NOT EXISTS "target"."dim_facility_type" (
  "facility_type_sk" text COLLATE "pg_catalog"."default" UNIQUE,
  "facility_type" text COLLATE "pg_catalog"."default"
);
ALTER TABLE "target"."dim_facility_type" OWNER TO "root";

CREATE TABLE IF NOT EXISTS "target"."dim_geography" (
  "geo_sk" text COLLATE "pg_catalog"."default" UNIQUE,
  "address" text COLLATE "pg_catalog"."default",
  "city" text COLLATE "pg_catalog"."default",
  "state" text COLLATE "pg_catalog"."default",
  "zip" text COLLATE "pg_catalog"."default",
  "latitude" numeric,
  "longitude" numeric
);
ALTER TABLE "target"."dim_geography" OWNER TO "root";

CREATE TABLE IF NOT EXISTS "target"."dim_inspection_result" (
  "inspection_result_sk" text COLLATE "pg_catalog"."default" UNIQUE,
  "inspection_result" text COLLATE "pg_catalog"."default"
);
ALTER TABLE "target"."dim_inspection_result" OWNER TO "root";

CREATE TABLE IF NOT EXISTS "target"."dim_inspection_type" (
  "inspection_type_sk" text COLLATE "pg_catalog"."default" UNIQUE,
  "inspection_type" text COLLATE "pg_catalog"."default"
);
ALTER TABLE "target"."dim_inspection_type" OWNER TO "root";

CREATE TABLE IF NOT EXISTS "target"."dim_restaurant" (
  "restaurant_sk" text COLLATE "pg_catalog"."default" UNIQUE,
  "dba_name" text COLLATE "pg_catalog"."default"
);
ALTER TABLE "target"."dim_restaurant" OWNER TO "root";

CREATE TABLE IF NOT EXISTS "target"."fact_food_inspection" (
  "inspection_sk" text COLLATE "pg_catalog"."default" UNIQUE,
  "restaurant_sk" text COLLATE "pg_catalog"."default",
  "geo_sk" text COLLATE "pg_catalog"."default",
  "inspection_result_sk" text COLLATE "pg_catalog"."default",
  "facility_type_sk" text COLLATE "pg_catalog"."default",
  "inspection_type_sk" text COLLATE "pg_catalog"."default",
  "inspection_date" text COLLATE "pg_catalog"."default",
  "inspection_id" int4,
  "license_no" int4
);
ALTER TABLE "target"."fact_food_inspection" OWNER TO "root";

CREATE TABLE IF NOT EXISTS "target"."fact_inspection_violation" (
  "inspection_violation_sk" text COLLATE "pg_catalog"."default" UNIQUE,
  "inspection_sk" text COLLATE "pg_catalog"."default",
  "violation" text COLLATE "pg_catalog"."default"
);
ALTER TABLE "target"."fact_inspection_violation" OWNER TO "root";

-- FK
ALTER TABLE target.fact_food_inspection DROP CONSTRAINT IF EXISTS Restaurant_FK;
ALTER TABLE target.fact_food_inspection 
ADD CONSTRAINT Restaurant_FK FOREIGN KEY (restaurant_sk) 
REFERENCES target.dim_restaurant (restaurant_sk) 
ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE target.fact_food_inspection DROP CONSTRAINT IF EXISTS Inspection_Type_FK;
ALTER TABLE target.fact_food_inspection 
ADD CONSTRAINT Inspection_Type_FK FOREIGN KEY (inspection_type_sk) 
REFERENCES target.dim_inspection_type (inspection_type_sk) 
ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE target.fact_food_inspection DROP CONSTRAINT IF EXISTS Geo_FK;
ALTER TABLE target.fact_food_inspection 
ADD CONSTRAINT Geo_FK FOREIGN KEY (geo_sk)
REFERENCES target.dim_geography (geo_sk) 
ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE target.fact_food_inspection DROP CONSTRAINT IF EXISTS Facility_FK;
ALTER TABLE target.fact_food_inspection 
ADD CONSTRAINT Facility_FK FOREIGN KEY (facility_type_sk) 
REFERENCES target.dim_facility_type (facility_type_sk) 
ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE target.fact_food_inspection DROP CONSTRAINT IF EXISTS Result_FK;
ALTER TABLE target.fact_food_inspection 
ADD CONSTRAINT Result_FK FOREIGN KEY (inspection_result_sk) 
REFERENCES target.dim_inspection_result (inspection_result_sk) 
ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE target.fact_food_inspection DROP CONSTRAINT IF EXISTS Inspection_FK;
ALTER TABLE target.fact_inspection_violation 
ADD CONSTRAINT Inspection_FK FOREIGN KEY (inspection_sk) 
REFERENCES target.fact_food_inspection (inspection_sk) 
ON DELETE CASCADE ON UPDATE CASCADE;