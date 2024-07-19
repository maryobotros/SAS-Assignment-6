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

/*order everything by empid and effdate then drag down store to all
the ones below it */
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


/*For each employee id, keep the last of the uniqure effdates
to get unique events (hire, term, loc)*/
data hr_transactions3;
	set hr_transactions2;
	by empid effdate;

	/*Remove it if it's not the last effdate*/
	if not last.effdate then delete;

	/*Figure out how to make empty rectypes into Loc*/
	temp_rectype = 'LOC';
	if missing(rectype) then rectype = temp_rectype;
	drop temp_rectype;
run;


/*For each empid and effdate(needed because posoiton can change with effdate)
drag down the first, last, position*/
data hr_transactions4;
	set hr_transactions3;
	by empid effdate;
	
	/*Retain first, last, and position to drag them down*/
	retain temp_first temp_last temp_position;

	/*Replace missing first last and position with dragged down temps*/
	if not missing(first)then temp_first = first;
	if missing(first) then first = temp_first;

	if not missing(last) then temp_last = last;
	if missing(last) then last = temp_last;

	if not missing(position) then temp_position = position;
	if missing(position) then position = temp_position;

	/*Drop all temp variables*/
	drop temp_first temp_last temp_position;
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

proc sql;
		CREATE TABLE merged_data as












