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

data hr_transactions5;
	set hr_transactions4;
	by empid effdate;
	
	*Retain first, last, and position to drag them down;
	retain temp_first temp_last temp_position;

	/*****Error: want to reset for each new effdate within an empid*****/
/*	if first.effdate then temp_first = '' and temp_last = '' and temp_position = '';*/

	*Replace missing first last and position with dragged down temps;
	if not missing(first)then temp_first = first;
	if missing(first) then first = temp_first;

	if not missing(last) then temp_last = last;
	if missing(last) then last = temp_last;

	if not missing(position) then temp_position = position;
	if missing(position) then position = temp_position;

	/*Drop all temp variables*/
	drop temp_first temp_last temp_position;
run;


/*------------------------ Question #3 ------------------------*/
*Bring the pay rates on to each of the records in the HR file. 

* sort pay and hr_transactions by position and effdate;
proc sort data=hr_transactions5 out=hr_transactions6;
	by position effdate;
run;

proc sort data=pay out=pay1;
	by position effdate;
run;


* Stack the pay file and the HR file;
data hr_transactions7;
	set pay1 hr_transactions6;
	by position effdate;
run;

*Drag down the last payrate to the missing rates for each position;
data hr_transactions8;
	set hr_transactions7;
	by position effdate;

	*Retain temp_rate variable;
	retain temp_rate;

	*Reset temp_rate for each new position to empty;
	if first.position then temp_rate = .;

	*If there is a rate then set temp_rate to that rate;
	*If the rate is missing then set the rate to the temp_rate;
	if rate then temp_rate = rate;
	if rate = . then rate = temp_rate;

	*Remove the temp_rate;
	drop temp_rate;
run;


*Clean the data to get rid of data from the pay dataset;
data hr_transactions9 hr_transactions9_delete;
	set hr_transactions8;
	
	if store ne . then output;
	else output hr_transactions9_delete;
run;


/*------------------------ Question #4 ------------------------*/
*Using one sort and one data step use retain to create a new variable called hiredate
for all records for each employee;

*Sort the data set first by empid and effdate;
proc sort data=hr_transactions9 out=hr_transactions10;
	by empid effdate;
run;

*Retain a variable hire_date variable;
data hr_transactions11;
	set hr_transactions10;
	by empid effdate;

	*Format hire_date and retain it;
	format hire_date mmddyy10.;
	retain hire_date;


	*Reset the hire_date for each new empid;
	if first.empid then call missing(hire_date);
	
	*If the rectyoe is HIRE then set the hire_date to the effdate;
	if rectype='HIRE' then hire_date = effdate;
run;


/*------------------------ Question #5 ------------------------*/
*Using one sort and one data step, create a variable called "TERMDATE";
proc sort data=hr_transactions11 out=hr_transactions12;
	by empid descending effdate;
run;

data hr_transactions13;
	set hr_transactions12;
	by empid descending effdate;

	*Format term_date and retain it;
	format term_date mmddyy10.;
	retain term_date;
	
	*For the first empid if the rectype is term then set the termdate to effdate;
	*Otherwise, that means the empid doesn't have a termdate and set it to 12/31/2999;
	if first.empid then do;
		if rectype = 'TERM' then term_date = effdate;
		else term_date = input("12/31/2999", mmddyy10.);
	end;
run;


/*------------------------ Question #6 ------------------------*/
*The attorney wants an emplyee head count on a monthly basic for each month from 
2001 to 2008;
*Run through the hr_transactions file and use a do loop and output to create a record
of each month an employee is active;

* Step 1: Create a promotion date for each employee

* Sort the hr_transactions data set by empid and effdate;
proc sort data=hr_transactions13 out=hr_transactions14;
	by empid effdate;
run;

* Create a flag for people who get a promotion;
data hr_transactions15;
	set hr_transactions14;
	by empid effdate;

	* Retain the promotion flag for the empid;
	retain promotion_flag;

	* Reset the promotion flag for each new empid;
	if first.empid then promotion_flag = .;
	if rectype = 'POS' then promotion_flag = 1;
	else promotion_flag = 0;
run;

*Reorder so that the flag for promotions is first for each empid;
proc sort data=hr_transactions15 out=hr_transactions16;
	by empid descending promotion_flag;
run;

data hr_transactions17;
	set hr_transactions16;
	by empid;

	* Format promo_date and retain it;
	format promo_date mmddyy10.;
	retain promo_date;

	* Clear the promotion_date for every new empid;
	if first.empid then call missing(promo_date);
	
	* If the rectype is POS then set the promotion_date to the effdate;
	if rectype = 'POS' then promo_date = effdate;
run;


* Step 2: Create an end date for each position an empid has;

* Sort by the data by promotion flag;
proc sort data=hr_transactions17 out=hr_transactions18;
	by empid effdate;
run;

data hr_transactions19;
	set hr_transactions18;
	by empid effdate;

	* Format and retain start and end date;
	format start_date mmddyy10.;
	format end_date mmddyy10.;
	retain start_date end_date;

	* Clear the start and end dates for each new empid;
	if first.empid then do;
		call missing(start_date);
		call missing(end_date);
	end;

	* If the employee doesn't have a promotion date then just set their start and end
	dates to the hire and term date;
	if promo_date = . then do;
		start_date = hire_date;
		end_date = term_date;
	end;


	* If the employee does have a promotion date the;
	if promo_date ne . then do;

		* If the rectype is hire then the start date is the hire date
		and the end date is the last day of the month prior to the promotion date
		This gets the first position for the promoted employee;
		if rectype = 'HIRE' then do; 
			start_date = hire_date;
			end_date = intnx('MONTH', promo_date, -1, 'E');; 
		end;

		* If the rectype is pos then the start date is the promotion date
		and the end date is the termination date
		This gets the second position for the promoted employee;
		if rectype = 'POS' then do;
			start_date = promo_date;
			end_date = term_date;
		end;
	end;
run;



* Step 3: Create the month_end_incumbent file that expands the hr file and 
Should have 2304 observations; 
data month_end_incumbent;
	set hr_transactions19;
	by empid start_date;

	format month mmddyy10.;

	* Reset current for each new empid. This may still not reset info for each new empid;
	if first.empid then do;
		current = .;
	end;

	*For each first start_date empid loop through all of the months between 2001 and 2008;
/*	if first.empid then do;*/
	if first.start_date then do i = 0 to 95;
		month = intnx('month', '31JAN2001'd,i,'same');
		if month >= start_date and month < = end_date then current = 1;
		else current = 0;
		output;
	end;
/*	end;*/
run;

/** Debugging;*/
/*data month_end_incumbent1;*/
/*	set month_end_incumbent;*/
/*	if first = 'VICTOR' then output;*/
/*run;*/


* Step 4: Group by month and then tally up total employees for that month

* Sort by month;
proc sort data=month_end_incumbent out=month_end_incumbent1;
	by month;
run;

* Create a running tally of employees for that each;
data month_end_incumbent2;
	set month_end_incumbent1;
	by month;

	* Retain total_employees for each month;
	retain total_employees;

	* Reset total_employees for each new month;
	if first.month then total_employees = .;

	* Add the current to total_emplyees to keep a running tally;
	total_employees + current;
run;

* Keep the last record for each month;
data month_end_incumbent3;
	set month_end_incumbent2;
	by month;

	if last.month then output;
run;


* Step 5: Find the total number of employees for each;
data month_end_incumbent4;
	set month_end_incumbent1;
	by month;

	* Retain each of the new variables;
	 retain total_employees servers kitchen locationmgr asstmgr;

	 * Reset each of the variables for each new month;
	 if first.month then do;
	 	total_employees = .;
		servers = .;
		kitchen = .;
		locationmgr = .;
		asstmgr = .;
	 end;

	 *Incrementing each of the variables if they are a current employee for
	the month_active and are the position;
	if current = 1 then total_employees + 1;
	if current = 1 and position = 'SERVER' then servers + 1;
	if current = 1 and position = 'KITCHEN' then kitchen + 1;
	if current = 1 and position = 'LOCATION MGR' then locationmgr + 1;
	if current = 1 and position = 'ASST MGR' then asstmgr + 1;
run;

* Keep the last record for each month;
data month_end_incumbent5;
	set month_end_incumbent4;
	by month;

	if last.month then output;
run;


* Clean the data set;
data month_end_incumbent6;
	set month_end_incumbent5;
	by month;

	keep total_employees month servers kitchen locationmgr asstmgr;
run;











