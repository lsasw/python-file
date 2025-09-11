--线路运行数据
select
    b.ID,
    replace(replace(replace(a."data_date", '-', ''), ':', ''), ' ', ''),
    a."ua"/1000
from
    (select a."data_date",a."ua",a."distline_name" from TEMP_DISTLINE_RUNDATA a group by a."data_date",a."ua",a."distline_name") a
    left join CIM_DISTMEASUREMENT b on concat(a."distline_name", '计量点') = b.NAME
    where substr(a."data_date",0,11)='2025-06-02'
    and b.COMPANY_ID=''
    order by a."data_date";

	
--配变运行数据
select
    b.ID,
    replace(replace(replace(a."data_date", '-', ''), ':', ''), ' ', ''),
    a."pa",
    a."pb",
    a."pc",
    a."p",
    a."qa",
    a."qb",
    a."qc",
    a."q",
    a."data_date",
    a."dittran_name",
    a."ia",
    a."ib",
    a."ic",
    a."i",
    a."ua",
    a."ub",
    a."uc"
from
    (select
         a.*,
         b."ia",
         b."ib",
         b."ic",
         b."i",
         b."ua",
         b."ub",
         b."uc"
     from
         (select
              *
          from
              (select
                   *,
                   ROW_NUMBER() OVER(PARTITION BY x."cust_no", x."cust_name", x."data_date" order by x."cust_no") AS rn
               from
                   TEMP_DISTTRAN_RUNDATA x
               where
                   substr(x."data_date", 0, 11) = '2025-06-10') a
          where
              rn = 1) a
         left join (select
                        *
                    from
                        (select
                             *,
                             ROW_NUMBER() OVER(PARTITION BY a."cust_no", a."cust_name", a."data_date" order by a."cust_no") AS rn
                         from
                             TEMP_DISTTRAN_VOL_RUNDATA a) a
                    where
                        rn = 1) b on a."cust_no" = b."cust_no" and a."data_date" = b."data_date") a
    left join CIM_DISTMEASUREMENT b on concat(a."dittran_name", '计量点') = b.NAME and b.COMPANY_ID='781160820799987712'
order by
    a."data_date";



--配变运行数据
select
    b.ID measurementId,
    replace(replace(replace(a."data_date", '-', ''), ':', ''), ' ', '') measTime,
    a."pa" P_A,
    a."pb" P_B,
    a."pc" P_C,
    a."p"  P,
    a."p"  PQ_P_A,
    a."p"  PQ_P_A_CALC,
    a."qa" Q_A,
    a."qb" Q_B,
    a."qc" Q_C,
    a."q" Q,
    a."q" PQ_P_R,
    a."q" PQ_P_R_CALC
from
    (select
         a.*,
         b."ia",
         b."ib",
         b."ic",
         b."i",
         b."ua",
         b."ub",
         b."uc"
     from
         (select
              *
          from
              (select
                   *,
                   ROW_NUMBER() OVER(PARTITION BY x."cust_no", x."cust_name", x."data_date" order by x."cust_no") AS rn
               from
                   TEMP_DISTTRAN_RUNDATA x
               where
                   substr(x."data_date", 0, 11) = '2025-06-02') a
          where
              rn = 1) a
         left join (select
                        *
                    from
                        (select
                             *,
                             ROW_NUMBER() OVER(PARTITION BY a."cust_no", a."cust_name", a."data_date" order by a."cust_no") AS rn
                         from
                             TEMP_DISTTRAN_VOL_RUNDATA a) a
                    where
                        rn = 1) b on a."cust_no" = b."cust_no" and a."data_date" = b."data_date") a
    left join CIM_DISTMEASUREMENT b on concat(a."dittran_name", '计量点') = b.NAME and b.COMPANY_ID='781160878035460096'
order by
    a."data_date";


    --线路运行数据
select
    b.ID measurementId,
    replace(replace(replace(a."data_date", '-', ''), ':', ''), ' ', '') measTime,
    a."ua"/1000 V
from
    (select a."data_date",a."ua",a."distline_name" from TEMP_DISTLINE_RUNDATA a group by a."data_date",a."ua",a."distline_name") a
    left join CIM_DISTMEASUREMENT b on concat(a."distline_name", '计量点') = b.NAME
    where substr(a."data_date",0,11)='2025-06-02'
    and b.COMPANY_ID='781160878035460096'
    order by a."data_date";
    
--     SELECT *from TEMP_DISTLINE_RUNDATA；
--     
--         SELECT *from  CIM_DISTMEASUREMENT  where name= '10kV东平线F11计量点'；
--         
        
  