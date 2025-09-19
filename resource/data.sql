insert into TEMP_DISTTRANSFORMER_LOWVOLT_DETAIL
select
    a.TRAN_ID,
    a.TRAN_NAME,
    a.DISTRIBUTION_LINE_ID,
    a.DISTRIBUTION_LINE_NAME,
    a.DATA_TIME,
    round(a.VOLTAGE, 2)                           highVolt,
    round(a.VOLTAGE / a.bianbi * 1000 / 1.732, 2) lowVolt,
    a."gear_type",
    a."gear_status"
from
    (select
         a.TRAN_ID,
         a.TRAN_NAME,
         a.DISTRIBUTION_LINE_ID,
         a.DISTRIBUTION_LINE_NAME,
         a.DATA_TIME,
         b."gear_type",
         b."gear_status",
     if(a.VOLTAGE = 0, 10, a.VOLTAGE) voltage,
         case
             when b."gear_type" = 5 and b."gear_status" = 1 then
                 26.25
             when b."gear_type" = 5 and b."gear_status" = 2 then
                 25.625
             when b."gear_type" = 5 and b."gear_status" = 3 then
                 25
             when b."gear_type" = 5 and b."gear_status" = 4 then
                 24.375
             when b."gear_type" = 5 and b."gear_status" = 5 then
                 23.75
             when b."gear_type" = 3 and b."gear_status" = 1 then
                 26.25
             when b."gear_type" = 3 and b."gear_status" = 2 then
                 25
             when b."gear_type" = 3 and b."gear_status" = 3 then
                 23.75
         end                          bianbi
     from
         (select
              a.TRAN_ID,
              a.TRAN_NAME,
              a.DISTRIBUTION_LINE_ID,
              a.DISTRIBUTION_LINE_NAME,
              a.DATA_TIME,
              a.VOLTAGE
          from
              LLT_DISTTRANSFORMERMINCALC20250601 a
          where
              a.DISTRIBUTION_LINE_NAME = '10kV东平线F11'
              and a.TRAN_TYPE = 'PUBLIC') a
         left join TEMP_GEAR_INFO b on a.TRAN_ID = b."disttran_id") a;



SELECT * from LLT_DISTTRANSFORMERMINCALC20250601;



SELECT * from TEMP_GEAR_INFO;

SELECT * from TEMP_DISTTRANSFORMER_LOWVOLT_DETAIL where TRAN_NAME = '东坡02公变';


db.getCollection("TEMP_DISTTRANSFORMER_LOWVOLT_DETAIL").find({ "DATA_TIME_SIGN": "2025-06-10" }).limit(1000).skip(0)



db.getCollection("LLT_DISTTRANSFORMERMINCALC20250604").find({ $and: [ { "companyId": 781162181876801536 }, { "tranName": "南边园村公变" } ] }
,{ 
    "_id": 0,
    "dataTime": 1 ,       // 排除 _id
    "tranName": 1,   // 包含 tranName
    "voltage": 1   // 包含 companyId
  }

).limit(1000).skip(0);
db.getCollection("LLT_DISTTRANSFORMERMINCALC20250604").find({ $and: [ { "companyId": 781162051559776256 }, { "tranName": "南边园村公变" } ] }
,{ 
    "_id": 0,        // 排除 _id
     "dataTime": 1 ,       // 排除 _id
    "tranName": 1,   // 包含 tranName
    "voltage": 1   // 包含 companyId
  }


).limit(1000).skip(0);


UPDATE TEMP_DISTTRAN_RUNDATA
SET "distline_name" = '10kV客北Ⅱ线F02'
WHERE
  "cust_no" = '0308100317307481';
  
  SELECT * from TEMP_DISTTRAN_RUNDATA
  WHERE
  "cust_no" = '0308100317355741';
  
UPDATE TEMP_DISTTRAN_RUNDATA
SET "dittran_name" = '本立村垦造水田（招家村）#2专变'
WHERE
  "cust_no" = '0308100317368321';



  --配变运行数据
SELECT
  b.ID measurementId,
  b.NAME,
  REPLACE(REPLACE(REPLACE(a."data_date", '-', ''), ':', ''), ' ', '') measTime,
  a."dittran_name",
  a."pa" P_A,
  a."pb" P_B,
  a."pc" P_C,
  a."p" P,
  a."p" PQ_P_A,
  a."p" PQ_P_A_CALC,
  a."qa" Q_A,
  a."qb" Q_B,
  a."qc" Q_C,
  a."q" Q,
  a."q" PQ_P_R,
  a."q" PQ_P_R_CALC
FROM
  (
    SELECT
      a.*
      ,
      b."ia",
      b."ib",
      b."ic",
      b."i",
      b."ua",
      b."ub",
      b."uc"
    FROM
      (
        SELECT
          *
        FROM
          (
            SELECT
              *,
              ROW_NUMBER () OVER (PARTITION BY x."cust_no", x."cust_name", x."data_date" ORDER BY x."cust_no") AS rn
            FROM
              TEMP_DISTTRAN_RUNDATA x
            WHERE
              substr(x."data_date", 0, 11) = '2025-08-01'
          ) a
        WHERE
          rn = 1
      ) a
      LEFT JOIN (
        SELECT
          *
        FROM
          (
            SELECT
              *,
              ROW_NUMBER () OVER (PARTITION BY a."cust_no", a."cust_name", a."data_date" ORDER BY a."cust_no") AS rn
            FROM
              TEMP_DISTTRAN_VOL_RUNDATA a
          ) a
        WHERE
          rn = 1
      ) b ON a."cust_no" = b."cust_no"
      AND a."data_date" = b."data_date"
  ) a
  LEFT  JOIN CIM_DISTMEASUREMENT b ON concat(a."dittran_name", '计量点') = b.NAME
  AND b.COMPANY_ID = '888415866973573120'
ORDER BY
  a."data_date";


  UPDATE TEMP_DISTTRAN_RUNDATA
SET "distline_name" = '10kV客北Ⅱ线F02'
WHERE
  "distline_name" IS NULL;
--线路运行数据
SELECT
  b.ID,
  REPLACE(REPLACE(REPLACE(a."data_date", '-', ''), ':', ''), ' ', ''),
  a."ua" / 1000
FROM
  (
    SELECT
      a."data_date",
      a."ua",
      a."distline_name"
    FROM
      TEMP_DISTLINE_RUNDATA a
    GROUP BY
      a."data_date",
      a."ua",
      a."distline_name"
  ) a
  LEFT JOIN CIM_DISTMEASUREMENT b ON concat(a."distline_name", '计量点') = b.NAME
WHERE
  substr(a."data_date", 0, 11) = '2025-08-01'
  AND b.COMPANY_ID = '888415866973573120'
ORDER BY
  a."data_date";
  
-- 1111111111
SELECT
  *
FROM
  CIM_DISTMEASUREMENT;
--配变运行数据
SELECT
  b.ID measurementId,
  b.NAME,
  REPLACE(REPLACE(REPLACE(a."data_date", '-', ''), ':', ''), ' ', '') measTime,
  a."pa" P_A,
  a."pb" P_B,
  a."pc" P_C,
  a."p" P,
  a."p" PQ_P_A,
  a."p" PQ_P_A_CALC,
  a."qa" Q_A,
  a."qb" Q_B,
  a."qc" Q_C,
  a."q" Q,
  a."q" PQ_P_R,
  a."q" PQ_P_R_CALC
FROM
  (
    SELECT
      a.*
--       ,
--       b."ia",
--       b."ib",
--       b."ic",
--       b."i",
--       b."ua",
--       b."ub",
--       b."uc"
    FROM
      (
        SELECT
          *
        FROM
          (
            SELECT
              *,
              ROW_NUMBER () OVER (PARTITION BY x."cust_no", x."cust_name", x."data_date" ORDER BY x."cust_no") AS rn
            FROM
              TEMP_DISTTRAN_RUNDATA x
            WHERE
              substr(x."data_date", 0, 11) = '2025-08-02'
          ) a
        WHERE
          rn = 1
      ) a
--       LEFT JOIN (
--         SELECT
--           *
--         FROM
--           (
--             SELECT
--               *,
--               ROW_NUMBER () OVER (PARTITION BY a."cust_no", a."cust_name", a."data_date" ORDER BY a."cust_no") AS rn
--             FROM
--               TEMP_DISTTRAN_VOL_RUNDATA a
--           ) a
--         WHERE
--           rn = 1
--       ) b ON a."cust_no" = b."cust_no"
--       AND a."data_date" = b."data_date"
  ) a
  LEFT JOIN CIM_DISTMEASUREMENT b ON concat(a."dittran_name", '计量点') = b.NAME
  AND b.COMPANY_ID = '888415866973573120'
ORDER BY
  a."data_date";
  
  

  SELECT ID 
FROM CIM_DISTMEASUREMENT 
WHERE PSR_ID IN (
    SELECT id 
    FROM CIM_DISTTRANSFORMER 
    WHERE COMPANY_ID = '888795392035508224'
      AND DISTRIBUTION_LINE_ID = '888827837720842240'
);

db.getCollection("cim_distmeasminute20250801").find({ "measurementId": Long("888827840426168360") }).limit(1000).skip(0)
UPDATE TEMP_DISTTRAN_RUNDATA
SET "distline_name" = '10kV客北Ⅱ线F02'
WHERE
  "cust_no" = '0308100317307481';
  
  SELECT * from TEMP_DISTTRAN_RUNDATA
  WHERE
  "cust_no" = '0308100317307481';
  
UPDATE TEMP_DISTTRAN_RUNDATA
SET "dittran_name" = '本立村垦造水田（文山村）#2专变'
WHERE
  "cust_no" = '0308100317307481';
  COMMIT;