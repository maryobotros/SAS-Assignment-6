/*SAS Assignment 6*/
/*Author: Maryo Botros*/
/*Jenner compatibility bundle: Question #2 from "SAS Assignment 6-V2.sas"
  (an alternate pass at the same problem as the original script) --
  stack hr_transactions with location_data, retain a temp_store cleared
  per empid via "if first.empid then temp_store = .;", then split the
  last-effdate-per-empid rows into TWO output datasets in a single DATA
  step (OUTPUT hr_transactions3; ... OUTPUT hr_transactions3_delete;),
  a technique the original script doesn't use. Ingestion uses INFILE
  DATALINES with rows taken verbatim from the repo's own CSVs in place of
  the original absolute paths; the retain/first.empid reset, the dual-
  output split, and the RECTYPE recode to 'LOC' are all unmodified.*/

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
Z294605,DAVID ,PERKINS,HIRE,11/5/2003,SERVER
Z294605,DAVID ,PERKINS,TERM,6/10/2005,SERVER
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
Z294605,1,11/5/2003
Z294605,4,1/3/2004
;
run;

/*------------------------ Question #2 ------------------------*/
*Put each employee's store location on to the HR file.

*Stack the HR file with the locations file which first requires sorting each
by ;
proc sort data=hr_transactions out=hr_transactions1;
	by empid effdate;
run;

proc sort data=location_data out=location_data1;
	by empid effdate;
run;

data hr_transactions1;
	set location_data1 hr_transactions1;
run;


*Sort the stacked file by empid and effdate to get all the employees
 grouped together and have them ordered by effdate;
proc sort data=hr_transactions1 out=hr_transactions1;
	by empid effdate;
run;

*Use retain to drag down the store location on to the records that
 do not already have a store location included.;
data hr_transactions2;
	set hr_transactions1;
	by empid effdate;

	retain temp_store;

	* Make sure to clear the temp_store with every first empid to avoid bleeding
	into the next empid and to handle case of missing first store;
	if first.empid then temp_store = .;

	*If the store exists, then keep that store and set it as the temp store;
	*Otherwise, if the store is empty then set the store as the temp store;
	if store then temp_store=store;
	if store=. then store=temp_store;


	*Drop the temp_store variable;
	drop temp_store;
run;

*For each employee, keep the last.effdate to get all unique effdates;
data hr_transactions3 hr_transactions3_delete;
	set hr_transactions2;
	by empid effdate;

	/*If it's the last effdate then output it otherwise put it in a different data set*/
	if last.effdate then output hr_transactions3;
	else output hr_transactions3_delete;
run;

*For employees who changed store locations recode the missing RECTYPE as LOC;
data hr_transactions4;
	set hr_transactions3;
	by empid effdate;

	* If the rectype is empty then set it as LOC;
	if rectype = '' then rectype = 'LOC';
run;

proc print data=hr_transactions4;
run;

proc print data=hr_transactions3_delete;
run;
