/*SAS Assignment 6*/
/*Author: Maryo Botros*/
/*Jenner compatibility bundle: the "SQL practice" section at the end of
  "SAS Assignment 6.sas" -- PROC SQL CREATE TABLE ... AS SELECT * FROM
  against both hr_transactions and location_data. INFILE was switched from
  the original absolute paths to INFILE DATALINES carrying rows taken
  verbatim from the repo's own CSVs; the two PROC SQL steps are unmodified.*/

data hr_transactions;
	format
		empid $char20.
		first $char20.
		last $char20.
		rectype $char20.
		effdate mmddyy10.
		position $char20.;

	informat
		empid $char20.
		first $char20.
		last $char20.
		rectype $char20.
		effdate mmddyy10.
		position $char20.;

	infile datalines
		lrecl=32767
		termstr=crlf
		dlm=','
		missover
		dsd;

	input
		empid
		first
		last
		rectype
		effdate
		position;
	datalines;
Z507208,AARON,WOOD,HIRE,2/6/2004,SERVER
Z304857,PHIL,JOHNSON,HIRE,3/5/2001,SERVER
Z304857,PHIL,JOHNSON,TERM,6/6/2006,SERVER
Z529415,PETE,JACKSON,HIRE,4/15/2001,SERVER
Z888277,JACK,SIMMONS,HIRE,9/22/2006,SERVER
;
run;

data location_data;
	infile datalines
		lrecl=32767
		termstr=crlf
		dlm=','
		missover
		dsd;

	format
		empid $char20.
		store best.
		effdate mmddyy10.;

	informat
		empid $char20.
		store best.
		effdate mmddyy10.;
	input
		empid
		store
		effdate;
	datalines;
Z507208,1,2/6/2004
Z304857,1,3/5/2001
Z529415,1,4/15/2001
Z888277,1,9/22/2006
;
run;

/*Sql practice*/
proc sql;
	CREATE TABLE hr_transactions_sql as
	SELECT * FROM hr_transactions;

quit;

proc sql;
	CREATE TABLE location_data_sql as
	SELECT * FROM location_data;
quit;

proc print data=hr_transactions_sql;
run;

proc print data=location_data_sql;
run;
