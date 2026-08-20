/*SAS Assignment 6*/
/*Author: Maryo Botros*/
/*Jenner compatibility bundle: Question #4 (retain a HIREDATE per employee)
  and Question #5 (retain a TERMDATE per employee, using a descending sort)
  from "SAS Assignment 6.sas". hr_transactions7 stands in for the output
  of Questions #1-#3 (a small hand-built sample of the same EMPID/RECTYPE/
  EFFDATE shape, with the same rows the repo's own HR transactions carry
  for a hired-then-terminated employee and a hired-then-promoted employee).
  The two PROC SORTs and both retain/format DATA steps are unmodified from
  the source.*/

/*Stand-in for the Question #1-3 output (hr_transactions7): two employees,
  one hired and terminated, one hired and promoted (never terminated).*/
data hr_transactions7;
	infile datalines
		lrecl=32767
		termstr=crlf
		dlm=','
		missover
		dsd;
	format empid $char20. rectype $char20. effdate mmddyy10.;
	informat empid $char20. rectype $char20. effdate mmddyy10.;
	input empid rectype effdate;
	datalines;
Z304857,HIRE,3/5/2001
Z304857,TERM,6/6/2006
Z980987,HIRE,6/19/2004
Z980987,POS,6/23/2005
;
run;

/*------------------------ Question #4 ------------------------*/
/*Using one sort and one data data step retain a new variabe called HIREDATE*/
proc sort data=hr_transactions7 out=hr_transactions7;
	by empid effdate;
run;

data hr_transactions8;
	set hr_transactions7;
	by empid effdate;

	/*Retain the hiredate and format it*/
	format hiredate mmddyy10.;
	retain hiredate;

	/*If the rectype is hire then set the hiredate to the effdate*/
	if rectype='HIRE' then hiredate=effdate;
run;


/*------------------------ Question #5 ------------------------*/
/*Using one sort and one data step add a termdate for the emplyees*/

/*Sort by empid and then by descending effdates so that term is the first*/
proc sort data=hr_transactions8 out=hr_transactions9;
	by empid descending effdate;
run;


data hr_transactions10;
	set hr_transactions9;
	by empid descending effdate;

	/*Retain the termdate and format it*/
	format termdate mmddyy10.;
	retain termdate;

	/*If the first record type is a termination then set the termdate to effdate.
	otherwise, set the termdate to 12/31/2999*/
	if first.empid then do;
		if rectype = 'TERM' then termdate = effdate;
		else termdate=input("12/31/2999", mmddyy10.);
	end;

run;

/*Reset the order so that they are sorted by each emplyee and then effdate*/
proc sort data=hr_transactions10 out=hr_transactions11;
	by empid effdate;
run;

proc print data=hr_transactions11;
run;
