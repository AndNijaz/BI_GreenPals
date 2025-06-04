SET search_path TO company_schema;

-- Prije updata, primjer: 
SELECT id AS company_id, name, location_id
FROM company_schema.companies
LIMIT 10;

-- Nakon updata:
UPDATE companies AS c
SET location_id = sub.id
FROM (
  SELECT company_id, MIN(id) AS id
  FROM company_locations
  GROUP BY company_id
) AS sub
WHERE c.id = sub.company_id;

SELECT id AS company_id, name, location_id
FROM company_schema.companies
LIMIT 10;
