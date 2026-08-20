/*SAS Assignment 6*/
/*Author: Maryo Botros*/
/*Jenner compatibility bundle: Question #7 (monthly headcount broken out
  by job title: SERVERS, KITCHEN, LOCATIONMGR, ASSTMGR) from
  "SAS Assignment 6.sas". month_end_incumbent2 stands in for the
  Question #6 output (one already-expanded month_active row per
  employee per month, current=1/0 flag, and POSITION) -- a small
  hand-built sample covering three positions across two months. The
  retain-four-counters-reset-per-month-active pattern and the final
  keep/last.month_active collapse are unmodified from the source.*/

/*Stand-in for the Question #6 output (month_end_incumbent2): 3 employees
  (SERVER, KITCHEN, LOCATION MGR) active in Jan 2001; the KITCHEN employee
  drops out (current=0) by Feb 2001.*/
data month_end_incumbent2;
	format month_active mmddyy10. position $char20.;
	informat month_active mmddyy10. position $char20.;
	infile datalines
		lrecl=32767
		termstr=crlf
		dlm=','
		missover
		dsd;
	input month_active position current;
	datalines;
1/31/2001,SERVER,1
1/31/2001,KITCHEN,1
1/31/2001,LOCATION MGR,1
2/28/2001,SERVER,1
2/28/2001,KITCHEN,0
2/28/2001,LOCATION MGR,1
;
run;

/*------------------------ Question #7 ------------------------*/
/*Create counters for each position and increment them each line using
month_end_incumbent2*/
data month_end_incumbent4;
	set month_end_incumbent2;
	by month_active;

	/*Retain each of these variables for each month_active*/
	retain total_employees servers kitchen locationmgr asstmgr;

	/*Reset each of the variables for each new month_active*/
	if first.month_active then do;
		total_employees = 0;
		servers = 0;
		kitchen = 0;
		locationmgr = 0;
		asstmgr = 0;
	end;

	/*Incrementing each of the variables if they are a current employee for
	the month_active and are the position*/
	if current = 1 then total_employees + 1;
	if current = 1 and position = 'SERVER' then servers + 1;
	if current = 1 and position = 'KITCHEN' then kitchen + 1;
	if current = 1 and position = 'LOCATION MGR' then locationmgr + 1;
	if current = 1 and position = 'ASST MGR' then asstmgr + 1;
run;


/*Clean the data*/
data month_end_incumbent5;
	set month_end_incumbent4;
	by month_active;

	/*Keep the last of each month_active*/
	if not last.month_active then delete;

	/*Keep only th enecessary variables*/
	keep month_active total_employees servers kitchen locationmgr asstmgr;
run;

proc print data=month_end_incumbent5;
run;
