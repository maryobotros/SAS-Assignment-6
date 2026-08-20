/*SAS Assignment 6*/
/*Author: Maryo Botros*/
/*Jenner compatibility bundle: Question #6 (expand each employee into one
  row per active month, then tally a monthly headcount) from
  "SAS Assignment 6.sas". hr_transactions14 stands in for the Question #1-5
  pipeline's output (a small hand-built HIREDATE/TERMDATE/PROMODATE sample
  covering both a still-active employee and a terminated one). The DO-loop
  month expansion (do i=0 to N; month_active = intnx(...); output; end;),
  the sort-then-tally retain pattern, and the final keep/last.month_active
  collapse are all unmodified from the source -- only the loop bound was
  reduced from 95 (96 months, 2001-2008) to 11 (12 months, just 2001) to
  keep this bundle's output small.*/

/*Stand-in for the Question #1-5 output (hr_transactions14): one employee
  hired in Jan 2001 and still active, one hired in Jan 2001 and terminated
  in June 2001.*/
data hr_transactions14;
	format empid $char20. position $char20. hiredate mmddyy10. termdate mmddyy10.;
	informat empid $char20. position $char20. hiredate mmddyy10. termdate mmddyy10.;
	input empid position hiredate termdate;
	datalines;
Z100001 SERVER 01/05/2001 12/31/2999
Z100002 KITCHEN 01/10/2001 06/15/2001
;
run;

/*------------------------ Question #6 ------------------------*/
/*Go through each empid and expand it with each month in the window.*/
data month_end_incumbent;
	set hr_transactions14;
	by empid;

	format month_active mmddyy10.;

	if first.empid then do i=0 to 11;
		month_active = intnx('month', '31JAN2001'd,i,'same');
		if month_active >= hiredate and month_active <= termdate then current = 1;
		else current = 0;
		output; /*This is what actually creates each of the new records*/
	end;
run;

/*Group by month_active*/
proc sort data=month_end_incumbent out=month_end_incumbent1;
	by month_active;
run;

data month_end_incumbent2;
	set month_end_incumbent1;
	by month_active;

	/*This will retain the total_emplyees variable for each month_active*/
	retain total_employees;

	/*This resets for each new month_active*/
	if first.month_active then total_employees = 0;

	/*This increments total_employees when the flag is read*/
	if current = 1 then total_employees + 1;
run;

/*Keeping each month which should output 12 results*/
data month_end_incumbent3;
	set month_end_incumbent2;
	by month_active;

	/*Keep the last of each month_active*/
	if not last.month_active then delete;

	/*Keep only th emonth and number of employees*/
	keep month_active total_employees;
run;

proc print data=month_end_incumbent3;
run;
