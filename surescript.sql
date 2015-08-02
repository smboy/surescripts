CREATE temporary TABLE `tmp_stg_ss_data` (
  `ncpdpid` varchar(10) DEFAULT NULL,
  `store_number` varchar(50) DEFAULT NULL,
  `store_name` varchar(100) DEFAULT NULL,
  `addressline1` varchar(200) DEFAULT NULL,
  `addressline2` varchar(200) DEFAULT NULL,
  `city` varchar(50) DEFAULT NULL,
  `state` varchar(5) DEFAULT NULL,
  `zip` varchar(12) DEFAULT NULL,
  `primary_phone` varchar(25) DEFAULT NULL,
  `fax` varchar(25) DEFAULT NULL,
  `active_start_time` varchar(20) DEFAULT NULL,
  `active_end_time` varchar(20) DEFAULT NULL,
  `text_service_level` varchar(100) DEFAULT NULL,
  `partner_account` varchar(100) DEFAULT NULL,
  `last_modified_date` varchar(20) DEFAULT NULL,
  `npi` varchar(20) DEFAULT NULL,
  `latitude` float,
  `longitude` float,
  `phone_number_id_fax` int,
  `phone_number_id_ph` int,
  `pharmacy_id` int,
  `zip_code_id` int
);

-- insert into temp table
insert into tmp_stg_ss_data
select
    a.*, null, null, null f
FROM stg_ss_data a;


-- Inserting data into production zip_codes table with new data (no updates required)
INSERT INTO zip_codes
            (zipcode, latitude, longitude) 
SELECT DISTINCT zip, latitude, longitude 
FROM   stg_ss_data a 
       LEFT JOIN zip_codes b ON a.zip = b.zipcode 
WHERE  b.zipcode IS NULL;

-- Below statements are used to update tmp_stg_ss_data table with actual IDs
-- This is a process to build the temp table first
-- this will be our source to insert/update phone_numbers and pharmacies

UPDATE tmp_stg_ss_data t
       JOIN zip_codes z ON z.zipcode = t.zip
SET    t.zip_code_id = z.id;

-- get fax numbers only (no phone numbers)
UPDATE tmp_stg_ss_data t
       JOIN phone_numbers p ON t.fax = p.number
SET    t.phone_number_id_fax = p.id,
       t.pharmacy_id = p.parent_id
WHERE  p.parent_type = 'Pharmacies'
       AND p.kind = 5
       AND p.is_preferred = 1;

-- get phone numbers only (no faxes)
UPDATE tmp_stg_ss_data t
       JOIN phone_numbers p ON t.primary_phone = p.number
SET    t.phone_number_id_ph = p.id,
       t.pharmacy_id = p.parent_id
WHERE  p.parent_type = 'Pharmacies'
       AND p.kind = 3
       AND p.is_preferred = 1;

-- Inserting into phone_numbers table new numbers(first insert fax numbers, kind = 5)

INSERT INTO phone_numbers
            (parent_id,
             parent_type,
             number,
             kind,
             is_preferred,
             created_at,
             updated_at)
SELECT DISTINCT -9 as parent_id,
                'Pharmacy',
                fax,
                5,
                1,
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP
FROM   tmp_stg_ss_data
WHERE  phone_number_id_fax IS NULL;

-- Inserting into phone_numbers table new numbers(insert phone numbers, kind = 3)

INSERT INTO phone_numbers
            (parent_id,
             parent_type,
             number,
             kind,
             is_preferred,
             created_at,
             updated_at)
SELECT DISTINCT -9 as parent_id,
                'Pharmacy',
                primary_phone,
                3,
                1,
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP
FROM   tmp_stg_ss_data
WHERE  phone_number_id_fax IS NULL;

-- update pharmacies table first
UPDATE pharmacies p
       JOIN tmp_stg_ss_data t ON t.pharmacy_id = p.id
SET p.name = t.store_name,
    p.ss_ncpdpid = t.ncpdpid
    p.ss_store_number = t.store_number,
    p.ss_active_start_time = t.active_start_time,
    p.ss_active_end_time = t.active_end_time,
    p.ss_partner_account = t.partner_account,
    p.ss_last_modified_date = t.last_modified_date,
    p.ss_npi = t.npi,
    p.updated_at = current_timestamp;

-- Now insert all the pharmacies that donot exist in the pharmacy table (temp table pharmacy_id is null)
INSERT INTO pharmacies (
        ss_ncpdpid, ss_store_number, ss_active_start_time, ss_active_end_time,
        ss_partner_account, ss_last_modified_date, ss_npi,
        created_at, updated_at)
SELECT distinct  ncpdpid, store_number, store_name, active_start_time, active_end_time,
       partner_account, last_modified_date, npi, current_timestamp, current_timestamp
from tmp_stg_ss_data
where pharmacy_id is null;

-- Now that we have pharmacy_id, update the temp table first with pharmacy_id

UPDATE tmp_stg_ss_data t
       JOIN pharmacies p ON t.ss_ncpdpid = p.ss_ncpdpid
SET    t.pharmacy_id = p.id
WHERE  t.pharmacy_id is null;

UPDATE phone_numbers p
        JOIN tmp_stg_ss_data t on p.number = t.primary_phone
SET    p.parent_id = t.pharmacy_id
WHERE  t.pharmacy_id IS NOT NULL
AND    p.parent_type = 'Pharmacy'
AND    p.kind = 3
AND    p.parent_id = -9
AND    p.updated_at = current_timestamp;

UPDATE phone_numbers p
        JOIN tmp_stg_ss_data t on p.number = t.fax
SET    p.parent_id = t.pharmacy_id
WHERE  t.pharmacy_id IS NOT NULL
AND    p.parent_type = 'Pharmacy'
AND    p.kind = 5
AND    p.parent_id = -9
AND    p.updated_at = current_timestamp;
