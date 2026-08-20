/*SAS Assignment 6*/
/*Author: Maryo Botros*/
/*Jenner compatibility bundle: Question #1 (comma-delimited infile ingestion
  with format/informat blocks) and Question #2 (stack + sort + retain
  drag-down of store location), adapted from "SAS Assignment 6.sas". The
  original INFILE statements pointed at an absolute path on the author's
  machine; here INFILE DATALINES supplies the identical CSV rows (a sample
  taken directly from the repo's own Munchies - HR Transactions.csv and
  Munchies - Location Data.csv) so the bundle is self-contained. The
  dlm=',' dsd infile options, the format/informat blocks, the stack, the
  sort, and the retain drag-down of STORE are all unmodified.*/

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
Z342381,TOM,BLAKE,HIRE,8/17/2000,KITCHEN
Z342381,TOM,BLAKE,TERM,5/4/2002,KITCHEN
Z831654,SUE,ANDOVER,HIRE,5/17/2003,KITCHEN
Z980987,LAURIE,ALEXANDER,HIRE,6/19/2004,KITCHEN
Z980987,LAURIE,ALEXANDER,POS,6/23/2005,SERVER
Z565332,VICTOR,DAVIS,HIRE,12/18/2004,SERVER
Z565332,VICTOR,DAVIS,POS,5/15/2005,LOCATION MGR
Z565332,VICTOR,DAVIS,TERM,7/6/2006,LOCATION MGR
Z863957,JAMIE,BLANK,HIRE,8/15/2004,SERVER
Z863957,JAMIE,BLANK,POS,8/27/2005,ASST MGR
;
run;

/*Read in Muchies - Location Data*/
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
Z342381,1,8/17/2000
Z342381,3,8/1/2001
Z831654,1,5/17/2003
Z980987,1,6/19/2004
Z565332,1,12/18/2004
Z863957,1,8/15/2004
;
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

proc print data=hr_transactions2;
run;
