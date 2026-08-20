/*SAS Assignment 6*/
/*Author: Maryo Botros*/
/*Jenner compatibility bundle: the PROC FREQ tabulation of RECTYPE from
  "Assignment 6 scratch.sas" (Question #1), plus the ingestion DATA step
  it runs against. INFILE was switched from the original absolute path to
  INFILE DATALINES carrying a sample of rows taken verbatim from the
  repo's own Munchies - HR Transactions.csv. The format/informat block and
  the PROC FREQ TABLE statement are unmodified.*/

data transactions;
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
Z529415,PETE,JACKSON,TERM,12/27/2002,SERVER
Z888277,JACK,SIMMONS,HIRE,9/22/2006,SERVER
Z342381,TOM,BLAKE,HIRE,8/17/2000,KITCHEN
Z342381,TOM,BLAKE,TERM,5/4/2002,KITCHEN
Z980987,LAURIE,ALEXANDER,HIRE,6/19/2004,KITCHEN
Z980987,LAURIE,ALEXANDER,POS,6/23/2005,SERVER
Z565332,VICTOR,DAVIS,HIRE,12/18/2004,SERVER
Z565332,VICTOR,DAVIS,POS,5/15/2005,LOCATION MGR
Z565332,VICTOR,DAVIS,TERM,7/6/2006,LOCATION MGR
Z863957,JAMIE,BLANK,HIRE,8/15/2004,SERVER
Z863957,JAMIE,BLANK,POS,8/27/2005,ASST MGR
;
run;

/*------------------------ Question #1 ------------------------*/
proc freq data=transactions;
	table rectype;
run;
