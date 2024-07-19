***DRAFT - CONFIDENTIAL ATTORNEY WORK PRODUCT***;
/*footnote "DRAFT-CONFIDENTIAL ATTORNEY WORK PRODUCT";*/

/*SAS Assignment 6*/
/*Author: Maryo Botros*/

/*------------------------ Question #1 ------------------------*/
/*Read in Munchies - HR Transactions*/
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

	infile "/mount/resdata/2. SAS Assignments & Training Data/Analyst Training Folders/Maryo Botros/Assignment 6/Munchies - HR Transactions.csv"
		lrecl=32767
		firstobs=2
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
run;

/*Read in Muchies - Location Data*/
data location_data;
	infile "/mount/resdata/2. SAS Assignments & Training Data/Analyst Training Folders/Maryo Botros/Assignment 6/Munchies - Location Data.csv"
		lrecl=32767
		firstobs=2
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
run;

/*Read in Munchies - Pay Scale*/
data pay;
	infile "/mount/resdata/2. SAS Assignments & Training Data/Analyst Training Folders/Maryo Botros/Assignment 6/Munchies - Pay Scale.csv"
		lrecl=32767
		firstobs=2
		termstr=crlf
		dlm=','
		missover
		dsd;

	format
		effdate mmddyy10.
		position $char20.
		rate best.;

	informat
		effdate mmddyy10.
		position $char20.
		rate best.;

	input
		effdate
		position
		rate;
run;


/*------------------------ Question #2 ------------------------*/
/*Put each employee's store location on to the main HR file (will need to
stack the HR file with the locations file)*/


/*Sort transactions*/
proc sort data=hr_transactions out=hr_transactions;
	by empid effdate;
run;

/*Sort location data*/
proc sort data=location_data out=location_data;
	by empid effdate;
run;

/*Stack transactions and location data*/
data hr_transactions1;
	set location_data hr_transactions;
run;

/*Sort the stacked data set so that the data is grouped by emplid*/
proc sort data=hr_transactions1 out=hr_transactions1;
	by empid effdate;
run;

data hr_transactions2;
	set hr_transactions1;

	/*Sort by empid and effdate*/
	by empid effdate;
	/*Retain the prev_score to drag it down to the record without a store*/
	retain prev_store;

	/*If it has a store then set the prev_store to store*/
	if store then prev_store = store;
	/*If it doesn't have a score then set the score as the previous score*/
	if store=. then store = prev_store;

	/*Drop the prev_score variable as it's no longer necessary*/
	drop prev_store;
run;
	

proc sql;
	CREATE TABLE hr_transactions_sql as 
	SELECT * FROM hr_transactions;

quit;

proc sql;
	CREATE TABLE location_data_sql as 
	SELECT * FROM location_data;
quit;

proc sql;
		CREATE TABLE merged_data as












