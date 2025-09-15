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


