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

/*------------------------ Question #3 ------------------------*/

/*Sort the new hr_transactions */
proc sort data=hr_transactions4 out=hr_transactions5;
	by position effdate;
run;

proc sort data=pay out=pay1;
	by position effdate;
run;


/*I treied doing the following two data seteps in one data step but it didn't work*/
data hr_transactions6;
	set pay1 hr_transactions5;
	by position effdate;

	retain temp_rate;

	/*This will reset the temp_rate for every first position
	and it is also an example of a retain down*/
	if first.position then call missing(temp_rate); 


	/*If the rate xists then set the temp_rate to the rate*/
	if not missing(rate) then temp_rate = rate;
	/*If the rate is missing (it's a .) then set the rate to the temp_rate*/
	if rate=. then rate=temp_rate;


	/*Delete the rows that were from the pay1 data set
	because we only need hr_tranmsaction data after we hav dragged pay info*/
	if store=. then delete;
	
	/*Drop temp variable*/
	drop temp_rate;
run;


/*Sort by position and then sort effdate descending */ 
/*and then sort back in the original order*/
/*retain down requires multiple data steps*/



/*------------------------ Question #4 ------------------------*/
/*Using one sort and one data data step retain a new variabe called HIREDATE*/
proc sort data=hr_transactions6 out=hr_transactions7;
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


/*------------------------ Question #6 ------------------------*/
/*The attorney wants to get an employee head count on monthly basis for each month
during the 2001 to 2008 class period. 
*/

/*First create a promotion date for all employees*/

/*Create a flag to detect if the empid has a promotion*/
data hr_transactions12;
	set hr_transactions11;
	by empid;
	
	/*Reatain the temporary promotion date*/
	retain temp_promodate;

	/*If they do have a promotion then set the flag to hello*/
	if rectype = 'POS' then temp_promodate = 'hello';
	else temp_promodate = .;

run;

/*Sort to get the prodate first for each empid (descending makes temp_promodate first)*/
proc sort data=hr_transactions12 out=hr_transactions13;
	by empid descending temp_promodate;
run;

/*Create a permenant promotion date that will be set for all records for the empid*/
data hr_transactions14;
	set hr_transactions13;
	by empid descending temp_promodate;

	format promodate mmddyy10.;
	retain promodate;

	
	if first.empid then do;
		if temp_promodate='hello' then promodate=effdate;
		if temp_promodate ne"hello" then promodate=.;
	end;

	/*Remove the temp promotion date variable*/
	drop temp_promodate;
run;

/*Reorder so that's by empid and effdate*/
proc sort data=hr_transactions14 out=hr_transactions15;
	by empid effdate;
run;


/*Go through each empid and expand it with each month between 2001 and 2008
Since there are 21 employees and 96 months, there weill be a total of 2016
results produced in this data set.*/
data month_end_incumbent;
	set hr_transactions15;
	by empid;

	format month_active mmddyy10.;

	if first.empid then do i=0 to 95;
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

/*Keeping each month which should output 96 results*/
data nonth_end_incumbent3;
	set month_end_incumbent2;
	by month_active;
	
	/*Keep the last of each month_active*/
	if not last.month_active then delete;

	/*Keep only th emonth and number of employees*/
	keep month_active total_employees;
run;














/*This code just outputs the total number of employees*/	
data num_of_employees;
	set hr_transactions15;
	by empid;

	
	retain total_employees 0;

	if first.empid then employee_number = 1;
	else employee_number = 0;

	if employee_number = 1 then total_employees + 1;
run;

proc freq data=hr_transactions15;





/*Making a data set that loops through years 2001 to 2008 and all months in those years*/
data want;
	do year = 2001 to 2008;
		do month = 1 to 12;
			date = mdy(month, 1, year);
			format date mmddyy10.;
			output;
		end;
	end;
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












