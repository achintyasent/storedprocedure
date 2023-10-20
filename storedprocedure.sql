
DROP PROCEDURE IF EXISTS form;

DELIMITER $$

CREATE PROCEDURE form(IN input JSON)

BEGIN

DECLARE t_timesheet_id INT;

DECLARE t_timesheet_name VARCHAR(40);

DECLARE t_week_start_date DATE;

DECLARE t_week_end_date DATE;

DECLARE t_contractor_name VARCHAR(40);

DECLARE t_pay_rate FLOAT;

DECLARE t_pay_frequency VARCHAR(40);

DECLARE t_working_hours FLOAT;

DECLARE t_action VARCHAR(40);

DECLARE t_D_id INT;

DECLARE i_invoice_date DATE;

DECLARE i_week_end_date DATE;

DECLARE i_customer_id INT;

DECLARE i_reason VARCHAR(250);

DECLARE i_invoice_id  INT;

DECLARE i_description  VARCHAR(250);

DECLARE i_hours  INT;

DECLARE i_pay_rate  FLOAT;

DECLARE i_tax_rate  FLOAT;

DECLARE agg VARCHAR(10);

DECLARE col VARCHAR(40);

DECLARE skip  INT;

DECLARE take  INT;


 DECLARE EXIT HANDLER FOR SQLEXCEPTION

      BEGIN

        

            GET DIAGNOSTICS CONDITION 1 @Message = MESSAGE_TEXT;


            SELECT 'fail' as status, @Message as message;


            ROLLBACK;           

    

      END;

SELECT JSON_UNQUOTE(JSON_EXTRACT(input,'$.t_id')),

             JSON_UNQUOTE(JSON_EXTRACT(input,'$.t_name')),

             JSON_UNQUOTE(JSON_EXTRACT(input,'$.action_flag')),

             JSON_UNQUOTE(JSON_EXTRACT(input,'$.t_startdate')),

             JSON_UNQUOTE(JSON_EXTRACT(input,'$.t_enddate')),

             JSON_UNQUOTE(JSON_EXTRACT(input,'$.t_cname')),

             JSON_UNQUOTE(JSON_EXTRACT(input,'$.t_payrate')),

             JSON_UNQUOTE(JSON_EXTRACT(input,'$.t_frequency')),

             JSON_UNQUOTE(JSON_EXTRACT(input,'$.t_units')),

             JSON_UNQUOTE(JSON_EXTRACT(input,'$.t_detid')),

             JSON_UNQUOTE(JSON_EXTRACT(input,'$.invoiceid')),

             JSON_UNQUOTE(JSON_EXTRACT(input,'$.invoicedate')),

             JSON_UNQUOTE(JSON_EXTRACT(input,'$.weekenddate')),

             JSON_UNQUOTE(JSON_EXTRACT(input,'$.customerid')),

             JSON_UNQUOTE(JSON_EXTRACT(input,'$.reason')),

             JSON_UNQUOTE(JSON_EXTRACT(input,'$.description')),

             JSON_UNQUOTE(JSON_EXTRACT(input,'$.hours')),

             JSON_UNQUOTE(JSON_EXTRACT(input,'$.payrate')),

             JSON_UNQUOTE(JSON_EXTRACT(input,'$.taxrate')),

             JSON_UNQUOTE(JSON_EXTRACT(input,'$.aggregate')),

             JSON_UNQUOTE(JSON_EXTRACT(input,'$.column')),
			 
			 JSON_UNQUOTE(JSON_EXTRACT(input,'$.skip')),
			 
			 JSON_UNQUOTE(JSON_EXTRACT(input,'$.take'))

      INTO  t_timesheet_id,

             t_timesheet_name,

             t_action,

             t_week_start_date,

             t_week_end_date,

             t_contractor_name,

             t_pay_rate,

             t_pay_frequency,

             t_working_hours,

             t_D_id,
             
             i_invoice_id,

             i_invoice_date,

             i_week_end_date,

             i_customer_id,

             i_reason,

             i_description,

             i_hours,

             i_pay_rate,

             i_tax_rate,

             agg,

             col,
			 
			 skip,
			 
			 take
;



IF t_action='insert-h' THEN 

 BEGIN 

 IF NOT EXISTS (SELECT 'a' FROM timesheet_histories WHERE timesheet_name=t_timesheet_name LIMIT 1) THEN

   BEGIN 

   INSERT INTO timesheet_histories(timesheet_name,week_start_date,week_end_date) VALUES

   (t_timesheet_name,t_week_start_date,t_week_end_date);

   END;

  ELSE

  BEGIN

  SELECT 'Error' as status,concat(t_timesheet_name,'already exists') as message;

  END;

  END IF;

 END;

END IF; 

IF t_action='insert-d' THEN 

BEGIN 

   INSERT INTO

   timesheet_details(contractor_name,pay_rate,pay_frequency,working_hours,timesheet_history_id,total_value) VALUES

   (t_contractor_name,t_pay_rate,t_pay_frequency,t_working_hours,t_timesheet_id,t_pay_rate*t_working_hours); 

 END;

END IF;

IF t_action='displayall' THEN

 BEGIN 

  SELECT * FROM timesheet_histories WHERE is_deleted=0;

 END;

END IF;

IF t_action='view' THEN

 BEGIN 

  SELECT * FROM timesheet_details WHERE timesheet_history_id=t_timesheet_id;

 END;

END IF;

IF t_action='getid' THEN

BEGIN 

SELECT timesheet_history_id FROM timesheet_histories WHERE timesheet_name=t_timesheet_name;

END;

END IF;

IF t_action='delete' THEN 

BEGIN

UPDATE timesheet_histories SET is_deleted=1 WHERE timesheet_history_id=t_timesheet_id;

UPDATE timesheet_details SET is_deleted=1 WHERE timesheet_history_id=t_timesheet_id;

END;

END IF;

IF t_action='i-display' THEN

BEGIN 

SELECT * FROM invoice WHERE is_deleted=0;

END;

END IF;

IF t_action='update' THEN

BEGIN

UPDATE timesheet_details
SET contractor_name=IFNULL(t_contractor_name,contractor_name),
    pay_rate = IFNULL(t_pay_rate,pay_rate),
    pay_frequency = IFNULL(t_pay_frequency,pay_frequency),
    working_hours = IFNULL(t_working_hours,working_hours),
    total_value =  IFNULL(t_pay_rate,pay_rate)*IFNULL(t_working_hours,working_hours)
WHERE detail_id=t_D_id;

END;

END IF;
 
IF t_action='i-view' THEN 

BEGIN 

SELECT * FROM invoice_line_item WHERE invoice_id=i_invoice_id;

END;

END IF;

IF t_action='i-getid' THEN

BEGIN

SELECT i.invoice_id,c.customer_name FROM invoice i LEFT JOIN customer c ON i.customer_id=c.customer_id;

END;

END IF;

IF t_action='aggregation' THEN

BEGIN


IF IFNULL(t_timesheet_name, '')='' THEN

 BEGIN
 

      SET @agg_columns = NULL;

      SELECT CONCAT(agg,"(",col,")") INTO @agg_columns;

      SET @query= CONCAT('SELECT ',@agg_columns,'as total FROM timesheet_details where is_deleted=0');


     PREPARE time_sheet_query FROM @query;

       EXECUTE time_sheet_query;

       DEALLOCATE PREPARE time_sheet_query;


 END;

ELSE

 BEGIN 
 

    SET @agg_columns = NULL;
      SET @t_timesheet_name = NULL;

      SET @t_timesheet_name = t_timesheet_name;

      SELECT CONCAT(agg,"(",col,")") INTO @agg_columns;

      SET @query= CONCAT('SELECT ',@agg_columns,'as total FROM (SELECT d.detail_id,d.contractor_name,d.pay_rate,d.pay_frequency,d.working_hours,h.timesheet_name,h.week_start_date,h.week_end_date,d.timesheet_history_id,total_value FROM timesheet_details as d INNER JOIN timesheet_histories as h ON d.timesheet_history_id=h.timesheet_history_id WHERE h.is_deleted=0)as t WHERE timesheet_name= ?');

      
       PREPARE time_sheet_query FROM @query;

       EXECUTE time_sheet_query USING @t_timesheet_name;

       DEALLOCATE PREPARE time_sheet_query;
						

 END;

END IF;

END;

END IF;

IF t_action='i-getdc' THEN 

BEGIN

SELECT * FROM timesheet_details WHERE contractor_name=t_contractor_name AND is_deleted=0;

END;

END IF;

IF t_action='i-getdi' THEN

BEGIN

SELECT * FROM (SELECT  A.timesheet_name, A.week_start_date, A.week_end_date, B.contractor_name, B.pay_rate, B.pay_frequency,B.working_hours,B.total_value  from timesheet_histories A JOIN timesheet_details B on A.timesheet_history_id = B.timesheet_history_id where A.is_deleted = '0')as result WHERE timesheet_name=t_timesheet_name;

END;

END IF;

IF t_action='i-gettotc' THEN

BEGIN

SELECT sum(total_value) as result  FROM timesheet_details WHERE contractor_name=t_contractor_name AND is_deleted=0;

END;

END IF;

IF t_action='i-gettoti' THEN

BEGIN

SELECT sum(total_value) as result FROM (SELECT  A.timesheet_name, A.week_start_date, A.week_end_date, B.contractor_name, B.pay_rate, B.pay_frequency,B.working_hours,B.total_value from timesheet_histories A JOIN timesheet_details B on A.timesheet_history_id = B.timesheet_history_id where A.is_deleted = '0')as result WHERE timesheet_name=t_timesheet_name ;

END;

END IF;

IF t_action='i-gettotb' THEN

BEGIN

SELECT sum(total_value) as result FROM (SELECT  A.timesheet_name, A.week_start_date, A.week_end_date, B.contractor_name, B.pay_rate, B.pay_frequency,B.working_hours,B.total_value from timesheet_histories A JOIN timesheet_details B on A.timesheet_history_id = B.timesheet_history_id where A.is_deleted = '0')as result WHERE  timesheet_name=t_timesheet_name and contractor_name=t_contractor_name ;

END;

END IF;

IF t_action='i-getdb' THEN

BEGIN

SELECT * FROM (SELECT  A.timesheet_name, A.week_start_date, A.week_end_date, B.contractor_name, B.pay_rate, B.pay_frequency,B.working_hours,B.total_value  from timesheet_histories A JOIN timesheet_details B on A.timesheet_history_id = B.timesheet_history_id where A.is_deleted = '0')as result WHERE timesheet_name=t_timesheet_name and contractor_name=t_contractor_name ;

END;

END IF;

IF t_action='i-delete' THEN 

BEGIN

UPDATE invoice SET is_deleted=1 WHERE invoice_id=i_invoice_id;

UPDATE invoice_line_item SET is_deleted=1 WHERE invoice_id=i_invoice_id;

END;

END IF;

IF t_action='i-update' THEN

BEGIN

UPDATE invoice_line_item
SET description= IFNULL(i_description,description),
    hours= IFNULL(i_hours,hours),
    pay_rate=IFNULL(i_pay_rate,pay_rate),
    tax_rate=IFNULL(i_tax_rate,tax_rate),
    total =IFNULL(i_hours,hours)*IFNULL(i_pay_rate,pay_rate)
WHERE invoice_line_item_id=i_invoice_id;

END;

END IF;

IF t_action='i-aggregation' THEN

BEGIN

IF IFNULL(i_invoice_id,0)=0 THEN

 BEGIN

      SET @agg_columns = NULL;

      SELECT CONCAT(agg,"(",col,")") INTO @agg_columns;

      SET @query= CONCAT('SELECT ',@agg_columns,'as total FROM invoice_line_item WHERE is_deleted=0');


     PREPARE time_sheet_query FROM @query;

      EXECUTE time_sheet_query;

      DEALLOCATE PREPARE time_sheet_query;


 END;

ELSE

 BEGIN 


    SET @agg_columns = NULL;
      SET @t_timesheet_name = NULL;

      SET @t_timesheet_name = i_invoice_id;

      SELECT CONCAT(agg,"(",col,")") INTO @agg_columns;

      SET @query= CONCAT('SELECT ',@agg_columns,'as total FROM invoice_line_item  WHERE is_deleted=0 AND invoice_id=?');

      
      PREPARE time_sheet_query FROM @query;

      EXECUTE time_sheet_query USING @t_timesheet_name;

      DEALLOCATE PREPARE time_sheet_query;
						

 END;

END IF;

END;

END IF;

IF t_action='displaybd' THEN

 BEGIN 

  SELECT *, COUNT(timesheet_history_id) OVER (ORDER BY (SELECT(0))) as total_count FROM timesheet_histories WHERE week_start_date BETWEEN t_week_start_date and t_week_end_date and is_deleted=0 LIMIT take OFFSET skip;

 END;

END IF;

END $$

DELIMITER $$