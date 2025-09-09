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